
import sys
import re
import json
import hashlib
import logging

from datetime import datetime, timezone

import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    LongType,
    IntegerType,
    BooleanType
)

from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "dea-calendly-marketing-hh"

LANDING_PREFIX = "landing/marketing_spend/"

BRONZE_PATH = (
    f"s3://{BUCKET}/bronze/marketing_spend/"
)

AWS_REGION = "us-east-1"

MAX_FILES = 5000
MAX_FILE_SIZE = 1024 * 1024
MAX_NEW_BYTES = 50 * 1024 * 1024


# ============================================================
# 2. INITIALIZE GLUE
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(
    "marketing-spend-landing-to-bronze"
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

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# 3. BRONZE SCHEMA
# ============================================================

BRONZE_SCHEMA = StructType([

    StructField("source_path", StringType(), False),

    StructField("source_file", StringType(), False),

    StructField("source_file_date", DateType(), False),

    StructField("content_hash", StringType(), False),

    StructField("source_size_bytes", LongType(), False),

    StructField("source_modified_at", TimestampType(), False),

    StructField("raw_json", StringType(), False),

    StructField("json_valid", BooleanType(), False),

    StructField("source_record_count", IntegerType(), True),

    StructField("ingested_at", TimestampType(), False)

])


# ============================================================
# 4. PARSE S3 OBJECT KEY
# ============================================================

def parse_source_key(key):

    pattern = (
        r"^landing/marketing_spend/"
        r"source_file=(spend_data_(\d{4}-\d{2}-\d{2})\.json)/"
        r"sha256=([0-9a-f]{64})\.json$"
    )

    match = re.fullmatch(pattern, key)

    if not match:
        raise ValueError(
            "Unexpected Marketing Spend S3 object key: "
            + key
        )

    source_file = match.group(1)

    source_date = datetime.strptime(
        match.group(2),
        "%Y-%m-%d"
    ).date()

    expected_hash = match.group(3)

    return source_file, source_date, expected_hash


def utc_naive(dt):

    return (
        dt.astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


# ============================================================
# 5. DISCOVER LANDING OBJECTS
# ============================================================

def list_landing_files():

    files = []

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=LANDING_PREFIX
    )

    for page in pages:

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            source_file, source_date, expected_hash = (
                parse_source_key(key)
            )

            files.append({

                "key": key,

                "source_path": f"s3://{BUCKET}/{key}",

                "source_file": source_file,

                "source_file_date": source_date,

                "expected_hash": expected_hash,

                "size": obj["Size"],

                "etag": obj["ETag"],

                "modified_at": obj["LastModified"]

            })

            if len(files) > MAX_FILES:

                raise RuntimeError(
                    "Landing file count exceeds the "
                    "configured limit."
                )

    return files


# ============================================================
# 6. READ EXISTING BRONZE METADATA
# ============================================================

def get_existing_paths():

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        logger.info(
            "Bronze Marketing Spend does not exist yet."
        )

        return set()

    existing_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
        .select("source_path")
        .distinct()
    )

    rows = (
        existing_df
        .limit(MAX_FILES + 1)
        .collect()
    )

    if len(rows) > MAX_FILES:

        raise RuntimeError(
            "Bronze source file count exceeds "
            "the configured driver-side limit."
        )

    return {
        row["source_path"]
        for row in rows
    }


# ============================================================
# 7. LOAD NEW FILES
# ============================================================

def load_new_files(new_files):

    rows = []

    total_bytes = 0

    ingestion_time = utc_naive(
        datetime.now(timezone.utc)
    )

    for file_info in new_files:

        key = file_info["key"]

        total_bytes += file_info["size"]

        if file_info["size"] > MAX_FILE_SIZE:

            raise RuntimeError(
                "Source file exceeds maximum size: "
                + key
            )

        if total_bytes > MAX_NEW_BYTES:

            raise RuntimeError(
                "New data exceeds the configured "
                "per-run memory limit."
            )

        obj = s3.get_object(
            Bucket=BUCKET,
            Key=key
        )

        raw_bytes = obj["Body"].read()

        # Detect modification between listing and reading.

        if (
            len(raw_bytes) != file_info["size"]
            or obj["ETag"] != file_info["etag"]
        ):

            raise RuntimeError(
                "Source object changed during ingestion: "
                + key
            )

        content_hash = hashlib.sha256(
            raw_bytes
        ).hexdigest()

        # Verify that the object matches its versioned key.

        if content_hash != file_info["expected_hash"]:

            raise RuntimeError(
                "Content SHA-256 does not match S3 key: "
                + key
            )

        raw_json = raw_bytes.decode("utf-8")

        # Bronze preserves the raw JSON. It does not
        # remove overlapping spend records.

        try:

            parsed = json.loads(raw_json)

            json_valid = (
                isinstance(parsed, list)
                and all(
                    isinstance(item, dict)
                    for item in parsed
                )
            )

            record_count = (
                len(parsed)
                if isinstance(parsed, list)
                else None
            )

        except json.JSONDecodeError:

            json_valid = False
            record_count = None

        rows.append((

            file_info["source_path"],

            file_info["source_file"],

            file_info["source_file_date"],

            content_hash,

            len(raw_bytes),

            utc_naive(file_info["modified_at"]),

            raw_json,

            json_valid,

            record_count,

            ingestion_time

        ))

    return rows


# ============================================================
# 8. WRITE BRONZE DELTA
# ============================================================

def write_bronze(rows):

    if not rows:

        logger.info("No new files to write.")
        return

    incoming_df = spark.createDataFrame(
        rows,
        schema=BRONZE_SCHEMA
    )

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        logger.info(
            "Creating Bronze Marketing Spend Delta Table."
        )

        (
            incoming_df.write
            .format("delta")
            .mode("append")
            .save(BRONZE_PATH)
        )

    else:

        logger.info(
            "Merging new source files into Bronze."
        )

        target = DeltaTable.forPath(
            spark,
            BRONZE_PATH
        )

        (
            target.alias("target")

            .merge(
                incoming_df.alias("source"),
                "target.source_path = source.source_path"
            )

            .whenNotMatchedInsertAll()

            .execute()
        )


# ============================================================
# 9. MAIN
# ============================================================

def main():

    logger.info(
        "Starting Marketing Spend Landing to Bronze."
    )

    landing_files = list_landing_files()

    logger.info(
        "Landing source versions discovered: %s",
        len(landing_files)
    )

    existing_paths = get_existing_paths()

    logger.info(
        "Already ingested source versions: %s",
        len(existing_paths)
    )

    new_files = [

        file_info

        for file_info in landing_files

        if file_info["source_path"]
        not in existing_paths

    ]

    logger.info(
        "New source versions selected: %s",
        len(new_files)
    )

    if not new_files:

        logger.info(
            "Bronze is up to date. No new files."
        )
        return

    rows = load_new_files(new_files)

    write_bronze(rows)

    logger.info(
        "Source versions processed: %s",
        len(rows)
    )

    logger.info(
        "Marketing Spend Bronze completed successfully."
    )


if __name__ == "__main__":

    main()

    job.commit()
