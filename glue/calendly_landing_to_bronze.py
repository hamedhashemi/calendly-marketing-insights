
import sys
import json
import hashlib
import logging

from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    TimestampType
)

from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "dea-calendly-marketing-hh"

LANDING_PREFIX = "landing/calendly/"

BRONZE_PATH = (
    f"s3://{BUCKET}/"
    "bronze/calendly/webhook_events/"
)

AWS_REGION = "us-east-1"

MAX_FILE_SIZE = 1024 * 1024

MAX_FILES_PER_RUN = 20000

MAX_NEW_BYTES_PER_RUN = 50 * 1024 * 1024

MAX_EXISTING_PATHS = 100000


# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(
    "calendly-landing-to-bronze"
)


# ============================================================
# 3. INITIALIZE GLUE AND SPARK
# ============================================================

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

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# 4. BRONZE SCHEMA
# ============================================================

BRONZE_SCHEMA = StructType([

    StructField(
        "source_path",
        StringType(),
        False
    ),

    StructField(
        "source_etag",
        StringType(),
        True
    ),

    StructField(
        "source_size_bytes",
        LongType(),
        True
    ),

    StructField(
        "source_modified_at",
        TimestampType(),
        True
    ),

    StructField(
        "webhook_event_type",
        StringType(),
        True
    ),

    StructField(
        "raw_json",
        StringType(),
        True
    ),

    StructField(
        "payload_hash",
        StringType(),
        True
    ),

    StructField(
        "json_valid",
        BooleanType(),
        True
    ),

    StructField(
        "ingested_at",
        TimestampType(),
        False
    )

])


# ============================================================
# 5. HELPER FUNCTIONS
# ============================================================

def to_utc_naive(value):

    if value is None:
        return None

    return (
        value.astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


def get_event_type_from_key(key):

    for section in key.split("/"):

        if section.startswith("event_type="):

            return section.split("=", 1)[1]

    return None


# ============================================================
# 6. DISCOVER LANDING FILES
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

            if not key.lower().endswith(".json"):
                continue

            files.append({

                "key": key,

                "source_path": (
                    f"s3://{BUCKET}/{key}"
                ),

                "source_etag": (
                    obj.get("ETag", "")
                    .strip('"')
                ),

                "source_size_bytes": obj["Size"],

                "source_modified_at": (
                    obj["LastModified"]
                )

            })

            if len(files) > MAX_FILES_PER_RUN:

                raise RuntimeError(
                    "Landing contains more files than "
                    "the configured profiling limit. "
                    "Use a batch/distributed ingestion "
                    "strategy before continuing."
                )

    return files


# ============================================================
# 7. READ ALREADY INGESTED FILES
# ============================================================

def get_existing_files():

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        logger.info(
            "Bronze Delta Table does not exist yet."
        )

        return {}

    logger.info(
        "Reading existing Bronze file metadata."
    )

    existing_df = (

        spark.read
        .format("delta")
        .load(BRONZE_PATH)

        .select(
            "source_path",
            "source_etag",
            "source_size_bytes"
        )

    )

    # This collection is bounded because this first
    # implementation targets a small educational dataset.

    existing_rows = (
        existing_df
        .limit(MAX_EXISTING_PATHS + 1)
        .collect()
    )

    if len(existing_rows) > MAX_EXISTING_PATHS:

        raise RuntimeError(
            "Bronze metadata exceeds the configured "
            "driver-side limit."
        )

    return {

        row["source_path"]: {

            "source_etag": row["source_etag"],

            "source_size_bytes": (
                row["source_size_bytes"]
            )

        }

        for row in existing_rows

    }


# ============================================================
# 8. SELECT NEW FILES
# ============================================================

def select_new_files(
    landing_files,
    existing_files
):

    new_files = []

    for file_info in landing_files:

        source_path = file_info[
            "source_path"
        ]

        existing = existing_files.get(
            source_path
        )

        if existing is None:

            new_files.append(
                file_info
            )

            continue

        # Landing objects should not be overwritten.
        # Detect modified objects instead of silently
        # skipping them.

        if (
            existing["source_etag"]
            != file_info["source_etag"]

            or

            existing["source_size_bytes"]
            != file_info["source_size_bytes"]
        ):

            raise RuntimeError(
                "Previously ingested Landing object "
                "appears to have changed: "
                f"{source_path}"
            )

    return new_files


# ============================================================
# 9. READ NEW JSON FILES
# ============================================================

def load_new_files(new_files):

    rows = []

    total_bytes = 0

    ingestion_time = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
    )

    for file_info in new_files:

        key = file_info["key"]

        file_size = file_info[
            "source_size_bytes"
        ]

        if file_size > MAX_FILE_SIZE:

            raise RuntimeError(
                "File exceeds maximum allowed size: "
                f"{key}"
            )

        total_bytes += file_size

        if total_bytes > MAX_NEW_BYTES_PER_RUN:

            raise RuntimeError(
                "New data exceeds the configured "
                "per-run memory limit."
            )

        response = s3.get_object(
            Bucket=BUCKET,
            Key=key
        )

        raw_bytes = (
            response["Body"].read()
        )

        # Detect a source object changing between
        # S3 listing and reading.

        response_etag = (
            response.get("ETag", "")
            .strip('"')
        )

        if (
            response_etag
            != file_info["source_etag"]

            or

            len(raw_bytes)
            != file_size
        ):

            raise RuntimeError(
                "Source object changed during ingestion: "
                f"{key}"
            )

        payload_hash = hashlib.sha256(
            raw_bytes
        ).hexdigest()

        # Preserve original JSON text.
        # Do not expose invitee details in logs.

        try:

            raw_json = raw_bytes.decode(
                "utf-8"
            )

        except UnicodeDecodeError:

            # Webhook JSON should be UTF-8.
            # Do not silently change its contents.

            raise RuntimeError(
                "Non-UTF-8 source object: "
                f"{key}"
            )

        json_valid = True

        webhook_event_type = (
            get_event_type_from_key(key)
        )

        try:

            parsed = json.loads(
                raw_json
            )

            if isinstance(parsed, dict):

                webhook_event_type = (
                    parsed.get("event")
                    or webhook_event_type
                )

            else:

                json_valid = False

        except json.JSONDecodeError:

            json_valid = False

        rows.append((

            file_info["source_path"],

            file_info["source_etag"],

            file_size,

            to_utc_naive(
                file_info["source_modified_at"]
            ),

            webhook_event_type,

            raw_json,

            payload_hash,

            json_valid,

            ingestion_time

        ))

    return rows


# ============================================================
# 10. WRITE BRONZE DELTA
# ============================================================

def write_bronze(rows):

    if not rows:

        logger.info(
            "No new files to write."
        )

        return 0

    incoming_df = spark.createDataFrame(
        rows,
        schema=BRONZE_SCHEMA
    )

    if not DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        logger.info(
            "Creating Bronze Delta Table."
        )

        (

            incoming_df.write
            .format("delta")
            .mode("errorifexists")
            .save(BRONZE_PATH)

        )

    else:

        logger.info(
            "Merging new files into existing Bronze."
        )

        bronze = DeltaTable.forPath(
            spark,
            BRONZE_PATH
        )

        (

            bronze.alias("target")

            .merge(
                incoming_df.alias("source"),

                (
                    "target.source_path = "
                    "source.source_path"
                )
            )

            .whenNotMatchedInsertAll()

            .execute()

        )

    return len(rows)


# ============================================================
# 11. MAIN PIPELINE
# ============================================================

def main():

    logger.info(
        "Starting Calendly Landing to Bronze job."
    )

    logger.info(
        "Bronze location: %s",
        BRONZE_PATH
    )

    landing_files = list_landing_files()

    logger.info(
        "Landing JSON files discovered: %s",
        len(landing_files)
    )

    existing_files = get_existing_files()

    logger.info(
        "Previously ingested source files: %s",
        len(existing_files)
    )

    new_files = select_new_files(
        landing_files,
        existing_files
    )

    logger.info(
        "New files selected: %s",
        len(new_files)
    )

    if not new_files:

        logger.info(
            "No new files. Bronze is up to date."
        )

        return

    rows = load_new_files(
        new_files
    )

    written_count = write_bronze(
        rows
    )

    logger.info(
        "New source files processed: %s",
        written_count
    )

    logger.info(
        "Calendly Bronze ingestion completed."
    )


# ============================================================
# 12. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()

    job.commit()