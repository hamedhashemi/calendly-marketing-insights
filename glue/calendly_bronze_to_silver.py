
import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "dea-calendly-marketing-hh"

BRONZE_PATH = (
    f"s3://{BUCKET}/"
    "bronze/calendly/webhook_events/"
)

SILVER_PATH = (
    f"s3://{BUCKET}/"
    "silver/calendly/booking/"
)

# Confirmed by coach:
# Breakthrough Session FB D2C Var

FACEBOOK_EVENT_TYPE_ID = (
    "d5c9e359-c580-4c11-8bf5-531a3de5ae5c"
)


# ============================================================
# 2. INITIALIZE AWS GLUE AND SPARK
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(
    "calendly-bronze-to-silver"
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
# 3. HELPER FUNCTIONS
# ============================================================

def json_field(path):
    """
    Extract a field from the raw Calendly JSON.
    """

    return F.get_json_object(
        F.col("raw_json"),
        path
    )


def extract_id(column):
    """
    Extract the last segment of a Calendly URI.

    Example:
    https://api.calendly.com/event_types/abc-123

    Result:
    abc-123
    """

    return F.regexp_extract(
        column,
        r"([^/]+)/?$",
        1
    )


# ============================================================
# 4. READ BRONZE DELTA TABLE
# ============================================================

def read_bronze():

    logger.info(
        "Reading Bronze Delta Table"
    )

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        raise RuntimeError(
            "Bronze Delta Table does not exist."
        )

    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    return bronze_df


# ============================================================
# 5. TRANSFORM BRONZE TO SILVER
# ============================================================

def transform_bookings(bronze_df):

    logger.info(
        "Starting Silver transformations"
    )

    # --------------------------------------------------------
    # 5.1 Filter valid invitee.created events
    # --------------------------------------------------------

    df = bronze_df.filter(

        (F.col("json_valid") == True)

        &

        (
            F.col("webhook_event_type")
            == "invitee.created"
        )

    )

    # --------------------------------------------------------
    # 5.2 Extract nested fields from raw JSON
    # --------------------------------------------------------

    df = (

        df

        .withColumn(
            "invitee_uri",
            json_field("$.payload.uri")
        )

        .withColumn(
            "event_uri",
            json_field(
                "$.payload.scheduled_event.uri"
            )
        )

        .withColumn(
            "event_type_uri",
            json_field(
                "$.payload.scheduled_event.event_type"
            )
        )

        .withColumn(
            "old_invitee_uri",
            json_field("$.payload.old_invitee")
        )

        .withColumn(
            "new_invitee_uri",
            json_field("$.payload.new_invitee")
        )

        .withColumn(
            "event_name",
            json_field(
                "$.payload.scheduled_event.name"
            )
        )

        .withColumn(
            "booking_created_at",
            F.to_timestamp(
                json_field("$.payload.created_at")
            )
        )

        .withColumn(
            "meeting_start_time",
            F.to_timestamp(
                json_field(
                    "$.payload.scheduled_event.start_time"
                )
            )
        )

        .withColumn(
            "meeting_end_time",
            F.to_timestamp(
                json_field(
                    "$.payload.scheduled_event.end_time"
                )
            )
        )

        .withColumn(
            "invitee_status",
            json_field("$.payload.status")
        )

        .withColumn(
            "invitee_timezone",
            json_field("$.payload.timezone")
        )

        .withColumn(
            "utm_source",
            json_field(
                "$.payload.tracking.utm_source"
            )
        )

        .withColumn(
            "utm_campaign",
            json_field(
                "$.payload.tracking.utm_campaign"
            )
        )

        .withColumn(
            "invitees_total",
            json_field(
                "$.payload.scheduled_event."
                "invitees_counter.total"
            ).cast("int")
        )

    )

    # --------------------------------------------------------
    # 5.3 Extract Employee information
    # --------------------------------------------------------

    # Parse the complete event_memberships array.

    df = df.withColumn(

        "event_memberships_array",

        F.from_json(

            json_field(
                "$.payload.scheduled_event."
                "event_memberships"
            ),

            "array<struct<user:string>>"

        )

    )

    # Number of employees assigned to the event.

    df = df.withColumn(

        "employee_count",

        F.coalesce(
            F.size(
                F.col("event_memberships_array")
            ),
            F.lit(0)
        )

    )

    # For the current Booking table, keep the first employee.
    # The full array remains available in Bronze.

    df = df.withColumn(

        "employee_uri",

        F.col(
            "event_memberships_array"
        ).getItem(0).getField("user")

    )

    # --------------------------------------------------------
    # 5.4 Extract Calendly business identifiers
    # --------------------------------------------------------

    df = (

        df

        .withColumn(
            "invitee_id",
            extract_id(
                F.col("invitee_uri")
            )
        )

        .withColumn(
            "event_id",
            extract_id(
                F.col("event_uri")
            )
        )

        .withColumn(
            "event_type_id",
            extract_id(
                F.col("event_type_uri")
            )
        )

        .withColumn(
            "old_invitee_id",
            extract_id(
                F.col("old_invitee_uri")
            )
        )

        .withColumn(
            "new_invitee_id",
            extract_id(
                F.col("new_invitee_uri")
            )
        )

        .withColumn(
            "employee_id",
            extract_id(
                F.col("employee_uri")
            )
        )

    )

    # --------------------------------------------------------
    # 5.5 Exclude synthetic test events
    # --------------------------------------------------------

    df = df.filter(

        ~F.coalesce(
            F.col("invitee_id").startswith(
                "test-invitee-"
            ),
            F.lit(False)
        )

        &

        ~F.coalesce(
            F.col("event_id").startswith(
                "test-event-"
            ),
            F.lit(False)
        )

    )

    # --------------------------------------------------------
    # 5.6 Basic Data Quality checks
    # --------------------------------------------------------

    df = df.filter(

        F.col("invitee_id").isNotNull()

        &

        (F.col("invitee_id") != "")

        &

        F.col("event_id").isNotNull()

        &

        (F.col("event_id") != "")

        &

        F.col("event_type_id").isNotNull()

        &

        (F.col("event_type_id") != "")

        &

        F.col("booking_created_at").isNotNull()

    )

    # --------------------------------------------------------
    # 5.7 Original Booking vs Rescheduling
    # --------------------------------------------------------

    # A newly created invitee record containing old_invitee
    # represents a booking linked to a previous reservation.

    df = (

        df

        .withColumn(

            "is_original_booking",

            (
                F.col("old_invitee_uri").isNull()

                |

                (
                    F.trim(
                        F.col("old_invitee_uri")
                    ) == ""
                )
            )

        )

        .withColumn(

            "booking_date",

            F.to_date(
                F.col("booking_created_at")
            )

        )

    )

    # --------------------------------------------------------
    # 5.8 Marketing Attribution
    # --------------------------------------------------------

    # Only the Event Type confirmed by the coach
    # is currently mapped to Facebook.

    df = (

        df

        .withColumn(

            "channel",

            F.when(

                F.col("event_type_id")
                == FACEBOOK_EVENT_TYPE_ID,

                F.lit("facebook_paid_ads")

            ).otherwise(

                F.lit(None).cast("string")

            )

        )

        .withColumn(

            "is_marketing_booking",

            F.col("channel").isNotNull()

        )

    )

    # --------------------------------------------------------
    # 5.9 Deduplicate by Invitee URI
    # --------------------------------------------------------

    # Multiple deliveries of the same invitee.created event
    # should produce only one Silver booking record.
    #
    # If multiple versions exist, select the latest one
    # based on source object modification time.

    window = (

        Window

        .partitionBy(
            "invitee_uri"
        )

        .orderBy(

            F.col(
                "source_modified_at"
            ).desc_nulls_last(),

            F.col(
                "source_path"
            ).desc()

        )

    )

    df = (

        df

        .withColumn(

            "row_number",

            F.row_number().over(
                window
            )

        )

        .filter(
            F.col("row_number") == 1
        )

        .drop(
            "row_number"
        )

    )

    # --------------------------------------------------------
    # 5.10 Select final Silver columns
    # --------------------------------------------------------

    silver_df = df.select(

        # Calendly identifiers

        "invitee_id",
        "event_id",
        "event_type_id",
        "event_name",

        # Rescheduling relationship

        "old_invitee_id",
        "new_invitee_id",

        # Marketing

        "channel",
        "is_marketing_booking",
        "is_original_booking",

        # Tracking

        "utm_source",
        "utm_campaign",

        # Booking and meeting time

        "booking_created_at",
        "booking_date",

        "meeting_start_time",
        "meeting_end_time",

        # Invitee status

        "invitee_status",
        "invitee_timezone",

        # Employee and group event information

        "employee_id",
        "employee_count",
        "invitees_total",

        # Data lineage

        "source_path",
        "source_modified_at",
        "payload_hash",

        F.current_timestamp().alias(
            "silver_updated_at"
        )

    )

    return silver_df


# ============================================================
# 6. WRITE SILVER DELTA TABLE
# ============================================================

def write_silver(silver_df):

    # --------------------------------------------------------
    # 6.1 Initial load
    # --------------------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        SILVER_PATH
    ):

        logger.info(
            "Silver table does not exist."
        )

        logger.info(
            "Creating Silver Delta Table."
        )

        (

            silver_df.write

            .format("delta")

            .mode("errorifexists")

            .save(SILVER_PATH)

        )

        logger.info(
            "Silver Delta Table created successfully."
        )

    # --------------------------------------------------------
    # 6.2 Incremental / repeat execution
    # --------------------------------------------------------

    else:

        logger.info(
            "Silver table exists."
        )

        logger.info(
            "Merging records into Silver Delta Table."
        )

        silver_table = DeltaTable.forPath(
            spark,
            SILVER_PATH
        )

        (

            silver_table.alias("target")

            .merge(

                silver_df.alias("source"),

                (
                    "target.invitee_id = "
                    "source.invitee_id"
                )

            )

            # Update only if the source record or its
            # derived business classification has changed.
            #
            # <=> is Spark SQL null-safe equality.

            .whenMatchedUpdateAll(

                condition="""

                    NOT (
                        target.payload_hash
                        <=> source.payload_hash
                    )

                    OR NOT (
                        target.event_type_id
                        <=> source.event_type_id
                    )

                    OR NOT (
                        target.channel
                        <=> source.channel
                    )

                    OR NOT (
                        target.is_original_booking
                        <=> source.is_original_booking
                    )

                    OR NOT (
                        target.source_path
                        <=> source.source_path
                    )

                """

            )

            .whenNotMatchedInsertAll()

            .execute()

        )

        logger.info(
            "Silver Delta MERGE completed."
        )


# ============================================================
# 7. SILVER DATA VALIDATION
# ============================================================

def validate_silver():

    logger.info(
        "Starting Silver Data Validation."
    )

    result_df = (

        spark.read

        .format("delta")

        .load(SILVER_PATH)

    )

    result_df.createOrReplaceTempView(
        "silver_booking"
    )

    # --------------------------------------------------------
    # TEST 1: Total rows and original bookings
    # --------------------------------------------------------

    print(
        "\n========== TEST 1: BOOKING COUNTS =========="
    )

    spark.sql("""

        SELECT

            COUNT(*) AS total_rows,

            COUNT(DISTINCT invitee_id)
                AS unique_invitees,

            SUM(
                CASE
                    WHEN is_original_booking
                    THEN 1
                    ELSE 0
                END
            ) AS original_bookings,

            SUM(
                CASE
                    WHEN NOT is_original_booking
                    THEN 1
                    ELSE 0
                END
            ) AS rescheduled_bookings

        FROM silver_booking

    """).show(
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 2: Confirmed Facebook Marketing Mapping
    # --------------------------------------------------------

    print(
        "\n========== TEST 2: MARKETING BOOKINGS =========="
    )

    spark.sql("""

        SELECT

            channel,

            COUNT(*) AS all_created,

            SUM(
                CASE
                    WHEN is_original_booking
                    THEN 1
                    ELSE 0
                END
            ) AS original_bookings,

            SUM(
                CASE
                    WHEN NOT is_original_booking
                    THEN 1
                    ELSE 0
                END
            ) AS rescheduled_bookings

        FROM silver_booking

        WHERE is_marketing_booking = true

        GROUP BY channel

        ORDER BY channel

    """).show(
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 3: Duplicate Invitee IDs
    # --------------------------------------------------------

    print(
        "\n========== TEST 3: DUPLICATE INVITEES =========="
    )

    duplicate_count = (

        result_df

        .groupBy(
            "invitee_id"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    print(
        f"Duplicate invitee IDs: {duplicate_count}"
    )

    if duplicate_count > 0:

        raise RuntimeError(
            "Silver contains duplicate invitee IDs."
        )

    # --------------------------------------------------------
    # TEST 4: Missing required fields
    # --------------------------------------------------------

    print(
        "\n========== TEST 4: REQUIRED FIELDS =========="
    )

    invalid_rows = (

        result_df.filter(

            F.col("invitee_id").isNull()

            |

            F.col("event_id").isNull()

            |

            F.col("event_type_id").isNull()

            |

            F.col("booking_created_at").isNull()

        )

        .count()

    )

    print(
        f"Rows with missing required fields: {invalid_rows}"
    )

    if invalid_rows > 0:

        raise RuntimeError(
            "Silver contains records with missing "
            "required business fields."
        )

    # --------------------------------------------------------
    # TEST 5: Employee membership distribution
    # --------------------------------------------------------

    print(
        "\n========== TEST 5: EMPLOYEE MEMBERSHIPS =========="
    )

    spark.sql("""

        SELECT

            employee_count,

            COUNT(*) AS booking_count

        FROM silver_booking

        GROUP BY employee_count

        ORDER BY employee_count

    """).show(
        truncate=False
    )

    # --------------------------------------------------------
    # TEST 6: Silver Date Range
    # --------------------------------------------------------

    print(
        "\n========== TEST 6: BOOKING DATE RANGE =========="
    )

    spark.sql("""

        SELECT

            MIN(booking_date) AS first_booking_date,

            MAX(booking_date) AS last_booking_date

        FROM silver_booking

    """).show(
        truncate=False
    )

    logger.info(
        "Silver Data Validation completed."
    )


# ============================================================
# 8. MAIN PIPELINE
# ============================================================

def main():

    logger.info(
        "Starting Calendly Bronze to Silver Job."
    )

    logger.info(
        "Bronze location: %s",
        BRONZE_PATH
    )

    logger.info(
        "Silver location: %s",
        SILVER_PATH
    )

    # Step 1: Read Bronze

    bronze_df = read_bronze()

    # Step 2: Transform to Silver

    silver_df = transform_bookings(
        bronze_df
    )

    # Step 3: Create or update Silver Delta

    write_silver(
        silver_df
    )

    # Step 4: Validate persisted Silver table

    validate_silver()

    logger.info(
        "Calendly Silver Job completed successfully."
    )


# ============================================================
# 9. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()

    job.commit()
