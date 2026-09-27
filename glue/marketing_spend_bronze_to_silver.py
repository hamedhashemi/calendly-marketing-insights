
import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    DecimalType
)

from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "dea-calendly-marketing-hh"

BRONZE_PATH = (
    f"s3://{BUCKET}/bronze/marketing_spend/"
)

SILVER_PATH = (
    f"s3://{BUCKET}/silver/marketing_spend/"
)

VALID_CHANNELS = [
    "facebook_paid_ads",
    "youtube_paid_ads",
    "tiktok_paid_ads"
]


# ============================================================
# 2. INITIALIZE GLUE
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(
    "marketing-spend-bronze-to-silver"
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

job.init(args["JOB_NAME"], args)


# ============================================================
# 3. SOURCE JSON SCHEMA
# ============================================================

SPEND_JSON_SCHEMA = ArrayType(

    StructType([

        StructField(
            "date",
            StringType(),
            True
        ),

        StructField(
            "channel",
            StringType(),
            True
        ),

        StructField(
            "spend",
            DecimalType(18, 2),
            True
        )

    ])

)


# ============================================================
# 4. READ BRONZE
# ============================================================

def read_bronze():

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        raise RuntimeError(
            "Marketing Spend Bronze Delta Table "
            "does not exist."
        )

    logger.info(
        "Reading Marketing Spend Bronze."
    )

    return (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )


# ============================================================
# 5. TRANSFORM AND VALIDATE
# ============================================================

def transform_spend(bronze_df):

    logger.info(
        "Starting Marketing Spend transformations."
    )

    # --------------------------------------------------------
    # 5.1 Validate Bronze source files
    # --------------------------------------------------------

    invalid_files = bronze_df.filter(

        ~F.coalesce(
            F.col("json_valid"),
            F.lit(False)
        )

        |

        F.col("source_file_date").isNull()

        |

        F.col("source_record_count").isNull()

        |

        (F.col("source_record_count") == 0)

    ).count()

    if invalid_files > 0:

        raise RuntimeError(
            f"{invalid_files} invalid source file(s) "
            "found in Bronze."
        )

    # --------------------------------------------------------
    # 5.2 Parse JSON arrays
    # --------------------------------------------------------

    parsed_df = bronze_df.withColumn(

        "spend_records",

        F.from_json(
            F.col("raw_json"),
            SPEND_JSON_SCHEMA
        )

    )

    parse_errors = parsed_df.filter(

        F.col("spend_records").isNull()

        |

        (
            F.size(F.col("spend_records"))
            != F.col("source_record_count")
        )

    ).count()

    if parse_errors > 0:

        raise RuntimeError(
            f"{parse_errors} source file(s) could "
            "not be parsed correctly."
        )

    # --------------------------------------------------------
    # 5.3 Flatten JSON arrays
    # --------------------------------------------------------

    df = (

        parsed_df

        .withColumn(
            "spend_record",
            F.explode("spend_records")
        )

        .select(

            "source_path",
            "source_file",
            "source_file_date",
            "source_modified_at",

            F.col(
                "spend_record.date"
            ).alias("spend_date_text"),

            F.col(
                "spend_record.channel"
            ).alias("channel"),

            F.col(
                "spend_record.spend"
            ).alias("spend_amount")

        )

    )

    # --------------------------------------------------------
    # 5.4 Convert data types
    # --------------------------------------------------------

    df = (

        df

        .withColumn(

            "spend_date",

            F.to_date(
                F.col("spend_date_text"),
                "yyyy-MM-dd"
            )

        )

        .withColumn(
            "channel",
            F.trim(F.col("channel"))
        )

        .withColumn(
            "currency",
            F.lit("USD")
        )

    )

    # --------------------------------------------------------
    # 5.5 Data quality validation
    # --------------------------------------------------------

    invalid_rows = df.filter(

        F.col("spend_date").isNull()

        |

        F.col("channel").isNull()

        |

        ~F.col("channel").isin(
            *VALID_CHANNELS
        )

        |

        F.col("spend_amount").isNull()

    ).count()

    if invalid_rows > 0:

        raise RuntimeError(
            f"{invalid_rows} invalid spend record(s) "
            "detected. Silver was not updated."
        )

    logger.info(
        "Spend data quality checks passed."
    )

    # --------------------------------------------------------
    # 5.6 Detect duplicates within the same file version
    # --------------------------------------------------------

    duplicate_source_keys = (

        df

        .groupBy(
            "source_path",
            "spend_date",
            "channel"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    if duplicate_source_keys > 0:

        raise RuntimeError(
            "A source file contains duplicate "
            "spend_date + channel records."
        )

    # --------------------------------------------------------
    # 5.7 Select the newest valid snapshot
    # --------------------------------------------------------

    # Priority:
    # 1. Newest source file publication date
    # 2. Newest retrieved object version
    # 3. Source path as deterministic tie-breaker

    ranking = (

        Window

        .partitionBy(
            "spend_date",
            "channel"
        )

        .orderBy(

            F.col(
                "source_file_date"
            ).desc(),

            F.col(
                "source_modified_at"
            ).desc(),

            F.col(
                "source_path"
            ).desc()

        )

    )

    latest_df = (

        df

        .withColumn(
            "row_number",
            F.row_number().over(ranking)
        )

        .filter(
            F.col("row_number") == 1
        )

        .drop("row_number")

    )

    # --------------------------------------------------------
    # 5.8 Final Silver schema
    # --------------------------------------------------------

    silver_df = latest_df.select(

        "spend_date",
        "channel",
        "spend_amount",
        "currency",

        "source_file",
        "source_file_date",
        "source_path",
        "source_modified_at",

        F.current_timestamp().alias(
            "silver_updated_at"
        )

    )

    return silver_df


# ============================================================
# 6. WRITE SILVER DELTA
# ============================================================

def write_silver(silver_df):

    # --------------------------------------------------------
    # Initial load
    # --------------------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        SILVER_PATH
    ):

        logger.info(
            "Creating Marketing Spend Silver Delta Table."
        )

        (
            silver_df.write
            .format("delta")
            .mode("append")
            .save(SILVER_PATH)
        )

        return

    # --------------------------------------------------------
    # Update existing Silver table
    # --------------------------------------------------------

    logger.info(
        "Merging Marketing Spend records into Silver."
    )

    target = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    (
        target.alias("target")

        .merge(

            silver_df.alias("source"),

            """
            target.spend_date = source.spend_date
            AND target.channel = source.channel
            """

        )

        .whenMatchedUpdateAll(

            condition="""

                NOT (
                    target.spend_amount
                    <=> source.spend_amount
                )

                OR NOT (
                    target.source_path
                    <=> source.source_path
                )

                OR NOT (
                    target.source_file_date
                    <=> source.source_file_date
                )

            """

        )

        .whenNotMatchedInsertAll()

        .execute()

    )


# ============================================================
# 7. VALIDATE PERSISTED SILVER
# ============================================================

def validate_silver():

    logger.info(
        "Starting Marketing Spend Silver validation."
    )

    result = (

        spark.read
        .format("delta")
        .load(SILVER_PATH)

    )

    result.createOrReplaceTempView(
        "silver_spend"
    )

    # --------------------------------------------------------
    # TEST 1: Overall statistics
    # --------------------------------------------------------

    print(
        "\n========== TEST 1: SPEND SUMMARY =========="
    )

    spark.sql("""

        SELECT

            COUNT(*) AS total_rows,

            COUNT(
                DISTINCT spend_date, channel
            ) AS unique_date_channel_keys,

            MIN(spend_date) AS earliest_spend_date,

            MAX(spend_date) AS latest_spend_date,

            SUM(spend_amount) AS total_spend_usd

        FROM silver_spend

    """).show(truncate=False)

    # --------------------------------------------------------
    # TEST 2: Channel distribution
    # --------------------------------------------------------

    print(
        "\n========== TEST 2: SPEND BY CHANNEL =========="
    )

    spark.sql("""

        SELECT

            channel,

            COUNT(*) AS days_available,

            SUM(spend_amount) AS total_spend_usd

        FROM silver_spend

        GROUP BY channel

        ORDER BY channel

    """).show(truncate=False)

    # --------------------------------------------------------
    # TEST 3: Duplicate business keys
    # --------------------------------------------------------

    print(
        "\n========== TEST 3: DUPLICATE KEYS =========="
    )

    duplicates = (

        result

        .groupBy(
            "spend_date",
            "channel"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .count()

    )

    print(
        f"Duplicate date + channel keys: {duplicates}"
    )

    if duplicates > 0:

        raise RuntimeError(
            "Silver contains duplicate "
            "spend_date + channel keys."
        )

    # --------------------------------------------------------
    # TEST 4: Latest available spend records
    # --------------------------------------------------------

    print(
        "\n========== TEST 4: LATEST SPEND RECORDS =========="
    )

    (

        result

        .orderBy(
            F.col("spend_date").desc(),
            F.col("channel").asc()
        )

        .select(
            "spend_date",
            "channel",
            "spend_amount",
            "source_file"
        )

        .show(
            12,
            truncate=False
        )

    )

    logger.info(
        "Marketing Spend Silver validation completed."
    )


# ============================================================
# 8. MAIN PIPELINE
# ============================================================

def main():

    logger.info(
        "Starting Marketing Spend Bronze to Silver."
    )

    bronze_df = read_bronze()

    silver_df = transform_spend(
        bronze_df
    )

    write_silver(
        silver_df
    )

    validate_silver()

    logger.info(
        "Marketing Spend Silver completed successfully."
    )


if __name__ == "__main__":

    main()

    job.commit()
