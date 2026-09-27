import json
import re
import hashlib
import logging

from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/"
)

INDEX_URL = SOURCE_BASE_URL + "file_index.json"

TARGET_BUCKET = "dea-calendly-marketing-hh"

TARGET_PREFIX = "landing/marketing_spend/"

AWS_REGION = "us-east-1"

MAX_FILE_SIZE = 1024 * 1024
MAX_FILES = 500
REQUEST_TIMEOUT = 30


# ============================================================
# LOGGING / AWS
# ============================================================

logger = logging.getLogger()

logger.setLevel(logging.INFO)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# HTTP DOWNLOAD
# ============================================================

def download_file(url):

    request = Request(
        url,
        headers={
            "User-Agent":
            "CalendlyMarketingInsightsLambda/1.0"
        }
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT
    ) as response:

        raw_bytes = response.read(
            MAX_FILE_SIZE + 1
        )

    if len(raw_bytes) > MAX_FILE_SIZE:

        raise RuntimeError(
            "Source file exceeds size limit."
        )

    return raw_bytes


# ============================================================
# VALIDATE SOURCE FILE NAME
# ============================================================

def validate_filename(filename):

    if not isinstance(filename, str):

        raise ValueError(
            "Invalid source filename."
        )

    match = re.fullmatch(
        r"spend_data_(\d{4}-\d{2}-\d{2})\.json",
        filename
    )

    if not match:

        raise ValueError(
            f"Unexpected source filename: {filename}"
        )

    file_date = datetime.strptime(
        match.group(1),
        "%Y-%m-%d"
    ).date()

    return file_date


# ============================================================
# GET SOURCE INDEX
# ============================================================

def get_file_index():

    logger.info(
        "Downloading Marketing Spend file_index.json"
    )

    raw_bytes = download_file(
        INDEX_URL
    )

    index = json.loads(
        raw_bytes
    )

    if not isinstance(index, dict):

        raise ValueError(
            "Unexpected file_index structure."
        )

    filenames = index.get("files")

    if not isinstance(filenames, list):

        raise ValueError(
            "file_index does not contain files array."
        )

    if len(filenames) > MAX_FILES:

        raise RuntimeError(
            "Source index exceeds configured limit."
        )

    for filename in filenames:

        validate_filename(
            filename
        )

    return sorted(
        set(filenames)
    )


# ============================================================
# CHECK S3 OBJECT
# ============================================================

def object_exists(key):

    try:

        s3.head_object(
            Bucket=TARGET_BUCKET,
            Key=key
        )

        return True

    except ClientError as error:

        error_code = error.response[
            "Error"
        ].get("Code")

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            return False

        raise


# ============================================================
# INGEST ONE FILE
# ============================================================

def ingest_file(filename):

    source_file_date = validate_filename(
        filename
    )

    source_url = (
        SOURCE_BASE_URL
        + quote(filename)
    )

    raw_bytes = download_file(
        source_url
    )

    # Validate JSON syntax.
    parsed = json.loads(
        raw_bytes
    )

    if not isinstance(parsed, list):

        raise ValueError(
            f"Spend file is not an array: {filename}"
        )

    content_hash = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    target_key = (

        TARGET_PREFIX

        + f"source_file={filename}/"

        + f"sha256={content_hash}.json"

    )

    if object_exists(target_key):

        logger.info(
            "SKIP | %s | already exists",
            filename
        )

        return "skipped"

    retrieved_at = datetime.now(
        timezone.utc
    ).isoformat()

    s3.put_object(

        Bucket=TARGET_BUCKET,

        Key=target_key,

        Body=raw_bytes,

        ContentType="application/json",

        Metadata={

            "source-file": filename,

            "source-file-date":
                source_file_date.isoformat(),

            "retrieved-at":
                retrieved_at,

            "content-sha256":
                content_hash

        }

    )

    logger.info(
        "STORED | %s | SHA256=%s",
        filename,
        content_hash[:12]
    )

    return "stored"


# ============================================================
# LAMBDA ENTRY POINT
# ============================================================

def lambda_handler(event, context):

    logger.info(
        "Starting automated Marketing Spend ingestion."
    )

    filenames = get_file_index()

    stored_count = 0
    skipped_count = 0
    failed_files = []

    for filename in filenames:

        try:

            result = ingest_file(
                filename
            )

            if result == "stored":

                stored_count += 1

            else:

                skipped_count += 1

        except Exception as error:

            logger.exception(
                "FAILED | %s",
                filename
            )

            failed_files.append({
                "filename": filename,
                "error_type":
                    type(error).__name__
            })

    summary = {

        "source_files_discovered":
            len(filenames),

        "stored":
            stored_count,

        "skipped":
            skipped_count,

        "failed":
            len(failed_files),

        "failed_files":
            failed_files

    }

    logger.info(
        "Ingestion summary: %s",
        json.dumps(summary)
    )

    # Important:
    # Step Functions must see a FAILURE
    # rather than a false-success Lambda execution.

    if failed_files:

        raise RuntimeError(
            f"{len(failed_files)} "
            "Marketing Spend file(s) failed."
        )

    return summary