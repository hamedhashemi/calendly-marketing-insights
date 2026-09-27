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

SILVER_BOOKING_PATH = (
    f"s3://{BUCKET}/silver/calendly/booking/"
)

BOOKING_TIME_GOLD_PATH = (
    f"s3://{BUCKET}/gold/booking_time_analysis/"
)

EMPLOYEE_LOAD_GOLD_PATH = (
    f"s3://{BUCKET}/gold/employee_meeting_load/"
)

BUSINESS_TIMEZONE = "America/New_York"


# ============================================================
# 2. INITIALIZE GLUE
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(
    "calendly-operational-gold"
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
# 3. READ SILVER
# ============================================================

def read_silver():

    logger.info(
        "Reading Calendly Silver Booking table."
    )

    return (
        spark.read
        .format("delta")
        .load(SILVER_BOOKING_PATH)
    )


# ============================================================
# 4. VALIDATE SOURCE SCHEMA
# ============================================================

def validate_source_schema(df):

    required_columns = {

        "invitee_id",
        "event_id",
        "event_type_id",

        "channel",
        "is_original_booking",

        "booking_created_at",

        "meeting_start_time",

        "employee_id",
        "employee_count",

        "old_invitee_id"

    }

    missing = (
        required_columns
        - set(df.columns)
    )

    if missing:

        raise RuntimeError(
            "Silver Booking is missing required columns: "
            + ", ".join(sorted(missing))
        )

    logger.info(
        "Silver source schema validation passed."
    )


# ============================================================
# 5. BUILD BOOKING TIME ANALYSIS
# ============================================================

def build_booking_time_analysis(booking_df):

    logger.info(
        "Building Booking Time Analysis Gold dataset."
    )

    # --------------------------------------------------------
    # Convert booking-created timestamp from UTC
    # into the business reporting timezone.
    #
    # Important:
    # This analyzes WHEN the lead created the booking,
    # not when the meeting itself takes place.
    # --------------------------------------------------------

    df = (

        booking_df

        .withColumn(

            "booking_local_ts",

            F.from_utc_timestamp(
                F.col("booking_created_at"),
                BUSINESS_TIMEZONE
            )

        )

        .withColumn(

            "report_date",

            F.to_date(
                F.col("booking_local_ts")
            )

        )

        .withColumn(

            "booking_hour",

            F.hour(
                F.col("booking_local_ts")
            )

        )

        .withColumn(

            "day_of_week",

            F.date_format(
                F.col("booking_local_ts"),
                "EEEE"
            )

        )

        # Monday = 1 ... Sunday = 7

        .withColumn(

            "day_of_week_number",

            (
                F.pmod(
                    F.dayofweek(
                        F.col("booking_local_ts")
                    )
                    + F.lit(5),
                    F.lit(7)
                )
                + F.lit(1)
            ).cast("int")

        )

    )

    # --------------------------------------------------------
    # SOURCE
    #
    # Only confirmed marketing mappings currently have
    # channel populated.
    #
    # Unknown / unconfirmed Calendly event types are
    # explicitly marked "unmapped", rather than being
    # incorrectly classified as Facebook/YouTube/TikTok.
    # --------------------------------------------------------

    df = (

        df

        .withColumn(

            "source",

            F.when(
                F.col("channel").isNotNull(),
                F.col("channel")
            ).otherwise(
                F.lit("unmapped")
            )

        )

        .withColumn(

            "source_mapping_status",

            F.when(
                F.col("channel").isNotNull(),
                F.lit("confirmed")
            ).otherwise(
                F.lit("unmapped")
            )

        )

    )

    # --------------------------------------------------------
    # Aggregate to:
    #
    # date + source + weekday + booking hour
    #
    # Primary booking_count = original bookings.
    #
    # We retain all-created and rescheduled counts
    # for transparency.
    # --------------------------------------------------------

    result = (

        df

        .groupBy(

            "report_date",

            "source",

            "source_mapping_status",

            "day_of_week",

            "day_of_week_number",

            "booking_hour"

        )

        .agg(

            F.count("*")
            .cast("long")
            .alias(
                "all_created_bookings"
            ),

            F.sum(

                F.when(
                    F.col(
                        "is_original_booking"
                    ) == True,
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )

            )
            .cast("long")
            .alias(
                "original_bookings"
            ),

            F.sum(

                F.when(
                    F.col(
                        "is_original_booking"
                    ) == False,
                    F.lit(1)
                ).otherwise(
                    F.lit(0)
                )

            )
            .cast("long")
            .alias(
                "rescheduled_bookings"
            )

        )

        # Primary dashboard metric

        .withColumn(

            "booking_count",

            F.col(
                "original_bookings"
            )

        )

        .withColumn(

            "gold_updated_at",

            F.current_timestamp()

        )

    )

    return result


# ============================================================
# 6. BUILD EMPLOYEE MEETING LOAD
# ============================================================

def build_employee_meeting_load(booking_df):

    logger.info(
        "Building Employee Meeting Load Gold dataset."
    )

    # --------------------------------------------------------
    # Current Silver stores one employee_id per booking.
    #
    # Previous profiling showed one employee membership
    # in current source data.
    #
    # Rather than silently undercounting if this changes,
    # fail the pipeline and require multi-employee modeling.
    # --------------------------------------------------------

    multi_employee_rows = (

        booking_df

        .filter(
            F.col("employee_count") > 1
        )

        .count()

    )

    if multi_employee_rows > 0:

        raise RuntimeError(

            f"{multi_employee_rows} booking record(s) "
            "contain multiple employees. "
            "Silver employee modeling must be expanded "
            "before calculating employee workload."

        )

    # --------------------------------------------------------
    # Required employee fields
    # --------------------------------------------------------

    invalid_employee_rows = (

        booking_df

        .filter(

            F.col("employee_id").isNull()

            |

            (F.trim(
                F.col("employee_id")
            ) == "")

            |

            F.col("event_id").isNull()

            |

            F.col(
                "meeting_start_time"
            ).isNull()

        )

        .count()

    )

    if invalid_employee_rows > 0:

        raise RuntimeError(

            f"{invalid_employee_rows} booking record(s) "
            "cannot be used for employee workload "
            "because required employee/event fields "
            "are missing."

        )

    # --------------------------------------------------------
    # RESCHEDULE CHAIN
    #
    # A new booking created through rescheduling contains
    # old_invitee_id.
    #
    # Any invitee_id later referenced as old_invitee_id
    # has been superseded by a newer booking.
    #
    # Remove those superseded invitee records before
    # creating scheduled-event workload.
    # --------------------------------------------------------

    superseded_invitees = (

        booking_df

        .filter(

            F.col(
                "old_invitee_id"
            ).isNotNull()

            &

            (
                F.trim(
                    F.col(
                        "old_invitee_id"
                    )
                ) != ""
            )

        )

        .select(

            F.col(
                "old_invitee_id"
            ).alias(
                "superseded_invitee_id"
            )

        )

        .distinct()

    )

    latest_booking_state = (

        booking_df.alias("booking")

        .join(

            superseded_invitees.alias(
                "superseded"
            ),

            F.col(
                "booking.invitee_id"
            )
            ==
            F.col(
                "superseded.superseded_invitee_id"
            ),

            "left_anti"

        )

    )

    # --------------------------------------------------------
    # IMPORTANT GRAIN:
    #
    # employee_id + event_id
    #
    # Group events can have multiple invitees but still
    # represent one employee meeting.
    #
    # Therefore we must NOT count invitee rows.
    # --------------------------------------------------------

    meeting_employee = (

        latest_booking_state

        .groupBy(
            "employee_id",
            "event_id"
        )

        .agg(

            F.max(
                "meeting_start_time"
            ).alias(
                "meeting_start_time"
            )

        )

        .withColumn(

            "meeting_local_ts",

            F.from_utc_timestamp(
                F.col("meeting_start_time"),
                BUSINESS_TIMEZONE
            )

        )

        .withColumn(

            "meeting_date",

            F.to_date(
                F.col("meeting_local_ts")
            )

        )

        .withColumn(

            "week_start",

            F.to_date(

                F.date_trunc(
                    "week",
                    F.col(
                        "meeting_local_ts"
                    )
                )

            )

        )

    )

    # --------------------------------------------------------
    # Weekly meetings per employee
    # --------------------------------------------------------

    weekly = (

        meeting_employee

        .groupBy(
            "employee_id",
            "week_start"
        )

        .agg(

            F.countDistinct(
                "event_id"
            )
            .cast("long")
            .alias(
                "scheduled_meeting_count"
            ),

            F.min(
                "meeting_date"
            ).alias(
                "first_meeting_date_in_week"
            ),

            F.max(
                "meeting_date"
            ).alias(
                "last_meeting_date_in_week"
            )

        )

    )

    # --------------------------------------------------------
    # Employee-level summary
    #
    # Requirement:
    # Avg Meetings per Week =
    # Total Meetings / Number of Weeks
    # --------------------------------------------------------

    employee_summary = (

        weekly

        .groupBy(
            "employee_id"
        )

        .agg(

            F.sum(
                "scheduled_meeting_count"
            )
            .cast("long")
            .alias(
                "employee_total_meetings"
            ),

            F.countDistinct(
                "week_start"
            )
            .cast("long")
            .alias(
                "observed_weeks"
            ),

            F.min(
                "week_start"
            ).alias(
                "first_observed_week"
            ),

            F.max(
                "week_start"
            ).alias(
                "last_observed_week"
            )

        )

        .withColumn(

            "avg_meetings_per_week",

            F.round(

                F.col(
                    "employee_total_meetings"
                )
                /
                F.col(
                    "observed_weeks"
                ),

                2

            ).cast(
                DecimalType(10, 2)
            )

        )

    )

    result = (

        weekly

        .join(

            employee_summary,

            on="employee_id",

            how="inner"

        )

        .select(

            "employee_id",

            "week_start",

            "scheduled_meeting_count",

            "employee_total_meetings",

            "observed_weeks",

            "avg_meetings_per_week",

            "first_meeting_date_in_week",

            "last_meeting_date_in_week",

            "first_observed_week",

            "last_observed_week",

            F.current_timestamp().alias(
                "gold_updated_at"
            )

        )

    )

    return result


# ============================================================
# 7. VALIDATE BOOKING TIME GOLD
# ============================================================

def validate_booking_time(
    booking_df,
    time_df
):

    logger.info(
        "Validating Booking Time Analysis."
    )

    duplicate_keys = (

        time_df

        .groupBy(

            "report_date",
            "source",
            "booking_hour"

        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    if duplicate_keys > 0:

        raise RuntimeError(
            "Booking Time Gold contains "
            "duplicate business keys."
        )

    expected_original = (

        booking_df

        .filter(
            F.col(
                "is_original_booking"
            ) == True
        )

        .count()

    )

    actual_original = (

        time_df

        .agg(

            F.sum(
                "original_bookings"
            ).alias("total")

        )

        .first()["total"]

    )

    actual_original = (
        actual_original or 0
    )

    if (
        int(actual_original)
        != int(expected_original)
    ):

        raise RuntimeError(

            "Booking Time reconciliation failed. "
            f"Silver original={expected_original}, "
            f"Gold original={actual_original}"

        )

    logger.info(
        "Booking Time validation passed."
    )


# ============================================================
# 8. VALIDATE EMPLOYEE GOLD
# ============================================================

def validate_employee_load(
    employee_df
):

    logger.info(
        "Validating Employee Meeting Load."
    )

    duplicate_keys = (

        employee_df

        .groupBy(
            "employee_id",
            "week_start"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    if duplicate_keys > 0:

        raise RuntimeError(
            "Employee Meeting Load contains "
            "duplicate employee/week keys."
        )

    invalid_metrics = (

        employee_df

        .filter(

            (
                F.col(
                    "scheduled_meeting_count"
                ) <= 0
            )

            |

            (
                F.col(
                    "observed_weeks"
                ) <= 0
            )

            |

            F.col(
                "avg_meetings_per_week"
            ).isNull()

        )

        .count()

    )

    if invalid_metrics > 0:

        raise RuntimeError(
            "Employee workload contains invalid metrics."
        )

    logger.info(
        "Employee Meeting Load validation passed."
    )


# ============================================================
# 9. WRITE GOLD TABLES
# ============================================================

def write_gold_tables(
    time_df,
    employee_df
):

    logger.info(
        "Writing Booking Time Gold Delta Table."
    )

    (
        time_df.write

        .format("delta")

        .mode("overwrite")

        .option(
            "overwriteSchema",
            "true"
        )

        .save(
            BOOKING_TIME_GOLD_PATH
        )
    )

    logger.info(
        "Writing Employee Meeting Load Delta Table."
    )

    (
        employee_df.write

        .format("delta")

        .mode("overwrite")

        .option(
            "overwriteSchema",
            "true"
        )

        .save(
            EMPLOYEE_LOAD_GOLD_PATH
        )
    )


# ============================================================
# 10. DISPLAY VALIDATION RESULTS
# ============================================================

def show_results():

    booking_time = (

        spark.read

        .format("delta")

        .load(
            BOOKING_TIME_GOLD_PATH
        )

    )

    employee_load = (

        spark.read

        .format("delta")

        .load(
            EMPLOYEE_LOAD_GOLD_PATH
        )

    )

    booking_time.createOrReplaceTempView(
        "gold_booking_time"
    )

    employee_load.createOrReplaceTempView(
        "gold_employee_load"
    )

    print(
        "\n========== TEST 1: BOOKING TIME SUMMARY =========="
    )

    spark.sql("""

        SELECT

            MIN(report_date) AS first_date,

            MAX(report_date) AS last_date,

            SUM(original_bookings)
                AS original_bookings,

            SUM(rescheduled_bookings)
                AS rescheduled_bookings,

            COUNT(DISTINCT booking_hour)
                AS observed_hours

        FROM gold_booking_time

    """).show(
        truncate=False
    )

    print(
        "\n========== TEST 2: BOOKINGS BY DAY =========="
    )

    spark.sql("""

        SELECT

            day_of_week_number,

            day_of_week,

            SUM(booking_count)
                AS bookings

        FROM gold_booking_time

        GROUP BY
            day_of_week_number,
            day_of_week

        ORDER BY
            day_of_week_number

    """).show(
        truncate=False
    )

    print(
        "\n========== TEST 3: BOOKINGS BY HOUR =========="
    )

    spark.sql("""

        SELECT

            booking_hour,

            SUM(booking_count)
                AS bookings

        FROM gold_booking_time

        GROUP BY booking_hour

        ORDER BY booking_hour

    """).show(
        24,
        truncate=False
    )

    print(
        "\n========== TEST 4: EMPLOYEE LOAD =========="
    )

    spark.sql("""

        SELECT

            employee_id,

            employee_total_meetings,

            observed_weeks,

            avg_meetings_per_week

        FROM gold_employee_load

        GROUP BY

            employee_id,

            employee_total_meetings,

            observed_weeks,

            avg_meetings_per_week

        ORDER BY
            employee_total_meetings DESC

    """).show(
        100,
        truncate=False
    )

    print(
        "\n========== TEST 5: WEEKLY EMPLOYEE LOAD =========="
    )

    spark.sql("""

        SELECT

            week_start,

            COUNT(DISTINCT employee_id)
                AS employees,

            SUM(scheduled_meeting_count)
                AS scheduled_meetings

        FROM gold_employee_load

        GROUP BY week_start

        ORDER BY week_start

    """).show(
        100,
        truncate=False
    )


# ============================================================
# 11. MAIN
# ============================================================

def main():

    logger.info(
        "Starting Calendly Operational Gold Job."
    )

    booking_df = read_silver()

    validate_source_schema(
        booking_df
    )

    booking_time_df = (
        build_booking_time_analysis(
            booking_df
        )
    )

    employee_load_df = (
        build_employee_meeting_load(
            booking_df
        )
    )

    # Validate before writing.

    validate_booking_time(
        booking_df,
        booking_time_df
    )

    validate_employee_load(
        employee_load_df
    )

    write_gold_tables(
        booking_time_df,
        employee_load_df
    )

    show_results()

    logger.info(
        "Calendly Operational Gold Job "
        "completed successfully."
    )


if __name__ == "__main__":

    main()

    job.commit()