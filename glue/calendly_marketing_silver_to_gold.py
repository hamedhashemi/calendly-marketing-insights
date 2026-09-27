import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "dea-calendly-marketing-hh"

BOOKING_SILVER_PATH = (
    f"s3://{BUCKET}/silver/calendly/booking/"
)

SPEND_SILVER_PATH = (
    f"s3://{BUCKET}/silver/marketing_spend/"
)

GOLD_PATH = (
    f"s3://{BUCKET}/gold/"
    "daily_marketing_performance/"
)

BUSINESS_TIMEZONE = "America/New_York"

# Calendly webhook was activated during Sep 21.
# Sep 22 is the first full business day of reliable coverage.

BOOKING_FULL_COVERAGE_START_DATE = "2026-09-22"


# ============================================================
# 2. INITIALIZE GLUE / SPARK
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(
    "calendly-marketing-silver-to-gold"
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext.getOrCreate()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


# ============================================================
# 3. READ SILVER TABLES
# ============================================================

def read_silver_tables():

    logger.info(
        "Reading Calendly Silver Booking table."
    )

    booking_df = (
        spark.read
        .format("delta")
        .load(BOOKING_SILVER_PATH)
    )

    logger.info(
        "Reading Marketing Spend Silver table."
    )

    spend_df = (
        spark.read
        .format("delta")
        .load(SPEND_SILVER_PATH)
    )

    return booking_df, spend_df


# ============================================================
# 4. AGGREGATE DAILY BOOKINGS
# ============================================================

def build_daily_bookings(booking_df):

    logger.info(
        "Building daily marketing booking metrics."
    )

    # --------------------------------------------------------
    # Keep only bookings already classified as marketing.
    #
    # At present this contains the coach-confirmed:
    #
    # Breakthrough Session FB D2C Var
    # -> facebook_paid_ads
    # --------------------------------------------------------

    df = booking_df.filter(
        (F.col("is_marketing_booking") == True)
        &
        F.col("channel").isNotNull()
    )

    # --------------------------------------------------------
    # Convert UTC booking timestamp to business timezone.
    #
    # This is important because a booking created at:
    #
    # 2026-09-23 02:00 UTC
    #
    # belongs to Sep 22 in Eastern Time.
    # --------------------------------------------------------

    df = df.withColumn(

        "report_date",

        F.to_date(

            F.from_utc_timestamp(
                F.col("booking_created_at"),
                BUSINESS_TIMEZONE
            )

        )

    )

    # --------------------------------------------------------
    # Daily aggregation
    # --------------------------------------------------------

    daily_df = (

        df

        .groupBy(
            "report_date",
            "channel"
        )

        .agg(

            F.count("*").alias(
                "all_created_bookings_raw"
            ),

            F.sum(

                F.when(
                    F.col("is_original_booking") == True,
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )

            ).cast("long").alias(
                "original_bookings_raw"
            ),

            F.sum(

                F.when(
                    F.col("is_original_booking") == False,
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )

            ).cast("long").alias(
                "rescheduled_bookings_raw"
            )

        )

    )

    return daily_df


# ============================================================
# 5. PREPARE DAILY SPEND
# ============================================================

def prepare_daily_spend(spend_df):

    logger.info(
        "Preparing daily Marketing Spend."
    )

    return spend_df.select(

        F.col("spend_date").alias(
            "report_date"
        ),

        "channel",

        "spend_amount",

        "currency",

        "source_file",

        "source_file_date",

        "source_path"

    )


# ============================================================
# 6. CREATE CHANNEL MAPPING STATUS
# ============================================================

def build_channel_mapping():

    # --------------------------------------------------------
    # Current approved project mapping.
    #
    # Facebook has a confirmed Calendly event mapping.
    #
    # YouTube and TikTok remain pending, so Gold must not
    # manufacture zero booking counts or misleading CPBs.
    # --------------------------------------------------------

    rows = [

        (
            "facebook_paid_ads",
            "confirmed"
        ),

        (
            "youtube_paid_ads",
            "pending"
        ),

        (
            "tiktok_paid_ads",
            "pending"
        )

    ]

    return spark.createDataFrame(
        rows,
        [
            "channel",
            "mapping_status"
        ]
    )


# ============================================================
# 7. BUILD GOLD TABLE
# ============================================================

def build_gold(
    booking_daily_df,
    spend_daily_df,
    mapping_df
):

    logger.info(
        "Joining daily bookings and spend."
    )

    # --------------------------------------------------------
    # FULL OUTER JOIN
    #
    # This preserves:
    #
    # - days with Spend but no booking record
    # - days with Booking but Spend not yet published
    #
    # This is important for identifying coverage gaps.
    # --------------------------------------------------------

    combined = (

        spend_daily_df.alias("spend")

        .join(

            booking_daily_df.alias("booking"),

            on=[
                "report_date",
                "channel"
            ],

            how="full_outer"

        )

        .join(

            mapping_df,

            on="channel",

            how="left"

        )

    )

    # Unknown channel should never silently appear as confirmed.

    combined = combined.withColumn(

        "mapping_status",

        F.coalesce(
            F.col("mapping_status"),
            F.lit("unmapped")
        )

    )

    # --------------------------------------------------------
    # Booking metrics
    #
    # Only confirmed channels receive numeric booking metrics.
    #
    # Pending mapping != zero bookings.
    # Pending mapping means "we do not yet know."
    # --------------------------------------------------------

    combined = (

        combined

        .withColumn(

            "all_created_bookings",

            F.when(

                F.col("mapping_status")
                == "confirmed",

                F.coalesce(
                    F.col(
                        "all_created_bookings_raw"
                    ),
                    F.lit(0)
                )

            ).otherwise(

                F.lit(None).cast("long")

            )

        )

        .withColumn(

            "original_bookings",

            F.when(

                F.col("mapping_status")
                == "confirmed",

                F.coalesce(
                    F.col(
                        "original_bookings_raw"
                    ),
                    F.lit(0)
                )

            ).otherwise(

                F.lit(None).cast("long")

            )

        )

        .withColumn(

            "rescheduled_bookings",

            F.when(

                F.col("mapping_status")
                == "confirmed",

                F.coalesce(
                    F.col(
                        "rescheduled_bookings_raw"
                    ),
                    F.lit(0)
                )

            ).otherwise(

                F.lit(None).cast("long")

            )

        )

    )

    # --------------------------------------------------------
    # Coverage rules
    # --------------------------------------------------------

    local_today = F.to_date(

        F.from_utc_timestamp(
            F.current_timestamp(),
            BUSINESS_TIMEZONE
        )

    )

    combined = (

        combined

        .withColumn(

            "booking_data_complete",

            (
                F.col("report_date")
                >= F.to_date(
                    F.lit(
                        BOOKING_FULL_COVERAGE_START_DATE
                    )
                )
            )

            &

            (
                F.col("report_date")
                < local_today
            )

        )

        .withColumn(

            "spend_available",

            F.col(
                "spend_amount"
            ).isNotNull()

        )

    )

    # --------------------------------------------------------
    # CPB STATUS
    # --------------------------------------------------------

    combined = combined.withColumn(

        "cpb_status",

        F.when(

            F.col("mapping_status")
            != "confirmed",

            F.lit("mapping_pending")

        )

        .when(

            F.col("booking_data_complete")
            == False,

            F.lit(
                "booking_data_incomplete"
            )

        )

        .when(

            F.col("spend_available")
            == False,

            F.lit("spend_missing")

        )

        .when(

            F.col("original_bookings")
            == 0,

            F.lit(
                "zero_original_bookings"
            )

        )

        .otherwise(

            F.lit("ready")

        )

    )

    # --------------------------------------------------------
    # COST PER BOOKING
    #
    # CPB exists only for rows whose status is ready.
    # --------------------------------------------------------

    combined = combined.withColumn(

        "cost_per_booking",

        F.when(

            F.col("cpb_status")
            == "ready",

            F.round(

                F.col("spend_amount")
                /
                F.col("original_bookings"),

                2

            ).cast(
                DecimalType(18, 2)
            )

        ).otherwise(

            F.lit(None).cast(
                DecimalType(18, 2)
            )

        )

    )

    # --------------------------------------------------------
    # Final Gold schema
    # --------------------------------------------------------

    gold_df = combined.select(

        "report_date",

        "channel",

        "mapping_status",

        "spend_amount",

        F.coalesce(
            F.col("currency"),
            F.lit("USD")
        ).alias(
            "currency"
        ),

        "all_created_bookings",

        "original_bookings",

        "rescheduled_bookings",

        "booking_data_complete",

        "spend_available",

        "cpb_status",

        "cost_per_booking",

        "source_file",

        "source_file_date",

        "source_path",

        F.current_timestamp().alias(
            "gold_updated_at"
        )

    )

    return gold_df


# ============================================================
# 8. WRITE GOLD DELTA
# ============================================================

def write_gold(gold_df):

    logger.info(
        "Writing Gold Delta Table."
    )

    # --------------------------------------------------------
    # Gold is a fully derived, relatively small dataset.
    #
    # We recompute it from the two Silver source tables and
    # atomically replace the current Gold snapshot.
    #
    # Delta still preserves transaction history.
    # --------------------------------------------------------

    (
        gold_df.write

        .format("delta")

        .mode("overwrite")

        .option(
            "overwriteSchema",
            "true"
        )

        .save(GOLD_PATH)
    )

    logger.info(
        "Gold Delta Table written successfully."
    )


# ============================================================
# 9. VALIDATE GOLD
# ============================================================

def validate_gold():

    logger.info(
        "Starting Gold validation."
    )

    result = (

        spark.read

        .format("delta")

        .load(GOLD_PATH)

    )

    result.createOrReplaceTempView(
        "gold_marketing"
    )

    # --------------------------------------------------------
    # TEST 1 - Overall Gold statistics
    # --------------------------------------------------------

    print(
        "\n========== TEST 1: GOLD SUMMARY =========="
    )

    spark.sql("""

        SELECT

            COUNT(*) AS total_rows,

            MIN(report_date)
                AS earliest_date,

            MAX(report_date)
                AS latest_date,

            COUNT(
                DISTINCT channel
            ) AS channels

        FROM gold_marketing

    """).show(
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 2 - Duplicate Business Keys
    # --------------------------------------------------------

    print(
        "\n========== TEST 2: DUPLICATE GOLD KEYS =========="
    )

    duplicate_keys = (

        result

        .groupBy(
            "report_date",
            "channel"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    print(
        "Duplicate report_date + channel keys: "
        f"{duplicate_keys}"
    )

    if duplicate_keys > 0:

        raise RuntimeError(
            "Gold contains duplicate business keys."
        )

    # --------------------------------------------------------
    # TEST 3 - CPB status distribution
    # --------------------------------------------------------

    print(
        "\n========== TEST 3: CPB STATUS =========="
    )

    spark.sql("""

        SELECT

            channel,

            mapping_status,

            cpb_status,

            COUNT(*) AS row_count

        FROM gold_marketing

        GROUP BY

            channel,
            mapping_status,
            cpb_status

        ORDER BY

            channel,
            cpb_status

    """).show(
        100,
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 4 - CPB should exist only on READY rows
    # --------------------------------------------------------

    print(
        "\n========== TEST 4: CPB CONSISTENCY =========="
    )

    invalid_cpb_rows = (

        result

        .filter(

            (
                F.col("cpb_status")
                == "ready"
            )

            &

            F.col(
                "cost_per_booking"
            ).isNull()

            |

            (
                F.col("cpb_status")
                != "ready"
            )

            &

            F.col(
                "cost_per_booking"
            ).isNotNull()

        )

        .count()

    )

    print(
        f"Invalid CPB rows: {invalid_cpb_rows}"
    )

    if invalid_cpb_rows > 0:

        raise RuntimeError(
            "Gold CPB consistency validation failed."
        )

    # --------------------------------------------------------
    # TEST 5 - Booking reconciliation
    # --------------------------------------------------------

    print(
        "\n========== TEST 5: FACEBOOK BOOKINGS =========="
    )

    spark.sql("""

        SELECT

            SUM(all_created_bookings)
                AS all_created,

            SUM(original_bookings)
                AS original,

            SUM(rescheduled_bookings)
                AS rescheduled

        FROM gold_marketing

        WHERE
            channel = 'facebook_paid_ads'

    """).show(
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 6 - Recent daily Gold output
    # --------------------------------------------------------

    print(
        "\n========== TEST 6: RECENT GOLD DATA =========="
    )

    (

        result

        .orderBy(

            F.col(
                "report_date"
            ).desc(),

            F.col(
                "channel"
            ).asc()

        )

        .select(

            "report_date",

            "channel",

            "mapping_status",

            "spend_amount",

            "original_bookings",

            "rescheduled_bookings",

            "booking_data_complete",

            "cpb_status",

            "cost_per_booking"

        )

        .show(
            30,
            truncate=False
        )

    )

    logger.info(
        "Gold validation completed successfully."
    )


# ============================================================
# 10. MAIN PIPELINE
# ============================================================

def main():

    logger.info(
        "Starting Calendly Marketing Gold Job."
    )

    booking_df, spend_df = (
        read_silver_tables()
    )

    booking_daily_df = (
        build_daily_bookings(
            booking_df
        )
    )

    spend_daily_df = (
        prepare_daily_spend(
            spend_df
        )
    )

    mapping_df = (
        build_channel_mapping()
    )

    gold_df = build_gold(

        booking_daily_df,

        spend_daily_df,

        mapping_df

    )

    write_gold(
        gold_df
    )

    validate_gold()

    logger.info(
        "Calendly Marketing Gold Job "
        "completed successfully."
    )


# ============================================================
# 11. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()

    job.commit()