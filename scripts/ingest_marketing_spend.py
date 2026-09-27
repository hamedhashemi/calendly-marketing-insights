
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
# 1. CONFIGURATION
# ============================================================

SOURCE_BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1."
    "amazonaws.com/calendly_spend_data/"
)

INDEX_URL = SOURCE_BASE_URL + "file_index.json"

TARGET_BUCKET = "dea-calendly-marketing-hh"

TARGET_PREFIX = "landing/marketing_spend/"

AWS_REGION = "us-east-1"

MAX_FILE_SIZE = 1024 * 1024  # 1 MB

MAX_FILES = 200

REQUEST_TIMEOUT = 30


# ============================================================
# 2. LOGGING AND AWS
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("marketing-spend-ingestion")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# 3. DOWNLOAD FILE
# ============================================================

def download_file(url):

    request = Request(
        url,
        headers={
            "User-Agent": "CalendlyMarketingInsights/1.0"
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
            "Source file exceeds the configured size limit."
        )

    return raw_bytes


# ============================================================
# 4. VALIDATE SOURCE FILENAME
# ============================================================

def validate_filename(filename):

    if not isinstance(filename, str):

        raise ValueError(
            "Invalid filename in source index."
        )

    match = re.fullmatch(
        r"spend_data_(\d{4}-\d{2}-\d{2})\.json",
        filename
    )

    if not match:

        raise ValueError(
            f"Unexpected filename: {filename}"
        )

    file_date = datetime.strptime(
        match.group(1),
        "%Y-%m-%d"
    ).date()

    return file_date


# ============================================================
# 5. EXTRACT FILE INDEX
# ============================================================

def get_file_index():

    logger.info(
        "Downloading file_index.json"
    )

    raw_bytes = download_file(
        INDEX_URL
    )

    index = json.loads(
        raw_bytes
    )

    if not isinstance(index, dict):

        raise ValueError(
            "Invalid file index structure."
        )

    filenames = index.get("files")

    if not isinstance(filenames, list):

        raise ValueError(
            "File index does not contain a files array."
        )

    if len(filenames) > MAX_FILES:

        raise RuntimeError(
            "File index exceeds the configured file limit."
        )

    # Validate all filenames before downloading files.

    for filename in filenames:

        validate_filename(
            filename
        )

    # Remove duplicate index entries and sort by date.

    filenames = sorted(
        set(filenames)
    )

    if not filenames:

        raise RuntimeError(
            "No spend files found in the source index."
        )

    logger.info(
        "Files discovered: %s",
        len(filenames)
    )

    logger.info(
        "Earliest source file: %s",
        filenames[0]
    )

    logger.info(
        "Latest source file: %s",
        filenames[-1]
    )

    return filenames


# ============================================================
# 6. CHECK WHETHER S3 OBJECT ALREADY EXISTS
# ============================================================

def object_exists(bucket, key):

    try:

        s3.head_object(
            Bucket=bucket,
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
# 7. INGEST ONE SOURCE FILE
# ============================================================

def ingest_file(filename):

    # Extract the publication date from the filename.

    source_file_date = validate_filename(
        filename
    )

    source_url = (
        SOURCE_BASE_URL
        + quote(filename)
    )

    # Download the original JSON bytes.

    raw_bytes = download_file(
        source_url
    )

    # Validate JSON syntax.
    # We preserve the original bytes in Landing.

    json.loads(
        raw_bytes
    )

    # Generate content-based version identifier.

    content_hash = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    # Each source file can have multiple content versions.

    target_key = (

        TARGET_PREFIX

        + f"source_file={filename}/"

        + f"sha256={content_hash}.json"

    )

    # Skip content versions already stored in Landing.

    if object_exists(
        TARGET_BUCKET,
        target_key
    ):

        logger.info(
            "SKIP | %s | Version already stored",
            filename
        )

        return "skipped"

    # Record the time this version was retrieved.

    retrieved_at = datetime.now(
        timezone.utc
    ).isoformat()

    # Preserve the original JSON bytes and source metadata.

    try:

        s3.put_object(

            Bucket=TARGET_BUCKET,

            Key=target_key,

            Body=raw_bytes,

            ContentType="application/json",

            Metadata={

                "source-file": filename,

                "source-file-date": (
                    source_file_date.isoformat()
                ),

                "retrieved-at": retrieved_at,

                "content-sha256": content_hash

            },

            # Avoid overwriting an existing content version.

            IfNoneMatch="*"

        )

    except ClientError as error:

        error_code = error.response[
            "Error"
        ].get("Code")

        if error_code in (
            "PreconditionFailed",
            "412"
        ):

            logger.info(
                "SKIP | %s | Version already exists",
                filename
            )

            return "skipped"

        raise

    logger.info(
        "STORED | %s | SHA256=%s",
        filename,
        content_hash[:12]
    )

    return "stored"


# ============================================================
# 8. MAIN INGESTION
# ============================================================

def main():

    logger.info(
        "Starting Marketing Spend Ingestion"
    )

    logger.info(
        "Target bucket: %s",
        TARGET_BUCKET
    )

    filenames = get_file_index()

    stored_count = 0
    skipped_count = 0
    failed_count = 0

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

            failed_count += 1

            logger.error(
                "FAILED | %s | %s: %s",
                filename,
                type(error).__name__,
                str(error)
            )

    logger.info(
        "========================================"
    )

    logger.info(
        "MARKETING SPEND INGESTION SUMMARY"
    )

    logger.info(
        "Source files discovered: %s",
        len(filenames)
    )

    logger.info(
        "New content versions stored: %s",
        stored_count
    )

    logger.info(
        "Existing versions skipped: %s",
        skipped_count
    )

    logger.info(
        "Failed files: %s",
        failed_count
    )

    logger.info(
        "========================================"
    )

    if failed_count > 0:

        raise RuntimeError(
            f"{failed_count} source file(s) failed ingestion."
        )


# ============================================================
# 9. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
