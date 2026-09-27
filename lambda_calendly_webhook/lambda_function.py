
import base64
import binascii
import boto3
import hashlib
import json
import os
import uuid

from datetime import datetime, timezone


# ============================================================
# Configuration
# ============================================================

s3 = boto3.client("s3")

BUCKET = os.environ["S3_BUCKET"]

ALLOWED_EVENTS = {
    "invitee.created",
    "invitee.canceled"
}

MAX_BODY_SIZE = 256 * 1024  # 256 KB


# ============================================================
# HTTP Response
# ============================================================

def response(status_code, message):

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message": message
        })
    }


# ============================================================
# Lambda Handler
# ============================================================

def lambda_handler(event, context):

    # --------------------------------------------------------
    # 1. Extract raw request body
    # --------------------------------------------------------

    body = event.get("body")

    if not isinstance(body, str):

        return response(
            400,
            "Missing or invalid request body"
        )

    try:

        if event.get("isBase64Encoded", False):

            raw_body = base64.b64decode(
                body,
                validate=True
            )

        else:

            raw_body = body.encode("utf-8")

    except (ValueError, binascii.Error):

        return response(
            400,
            "Invalid request encoding"
        )

    # --------------------------------------------------------
    # 2. Check payload size
    # --------------------------------------------------------

    if len(raw_body) > MAX_BODY_SIZE:

        return response(
            413,
            "Request body too large"
        )

    # --------------------------------------------------------
    # 3. Parse JSON
    # --------------------------------------------------------

    try:

        payload = json.loads(raw_body)

    except (json.JSONDecodeError, UnicodeDecodeError):

        return response(
            400,
            "Invalid JSON payload"
        )

    if not isinstance(payload, dict):

        return response(
            400,
            "Expected a JSON object"
        )

    # --------------------------------------------------------
    # 4. Validate event structure
    # --------------------------------------------------------

    webhook_event = payload.get("event")

    if webhook_event not in ALLOWED_EVENTS:

        return response(
            400,
            "Unsupported webhook event"
        )

    invitee_payload = payload.get("payload")

    if not isinstance(invitee_payload, dict):

        return response(
            400,
            "Missing invitee payload"
        )

    invitee_uri = invitee_payload.get("uri")

    if not isinstance(invitee_uri, str) or not invitee_uri:

        return response(
            400,
            "Missing invitee URI"
        )

    # --------------------------------------------------------
    # 5. Generate ingestion metadata
    # --------------------------------------------------------

    ingestion_time = datetime.now(timezone.utc)

    request_id = str(uuid.uuid4())

    payload_hash = hashlib.sha256(
        raw_body
    ).hexdigest()

    # --------------------------------------------------------
    # 6. Build S3 object key
    # --------------------------------------------------------

    s3_key = (
        "landing/calendly/"
        f"event_type={webhook_event}/"
        f"year={ingestion_time:%Y}/"
        f"month={ingestion_time:%m}/"
        f"day={ingestion_time:%d}/"
        f"{request_id}.json"
    )

    # --------------------------------------------------------
    # 7. Store raw JSON in S3
    # --------------------------------------------------------

    try:

        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=raw_body,
            ContentType="application/json",
            Metadata={
                "ingestion-time": ingestion_time.isoformat(),
                "payload-sha256": payload_hash
            }
        )

    except Exception:

        print(
            "Webhook ingestion failed: S3 write error"
        )

        return response(
            500,
            "Storage error"
        )

    # --------------------------------------------------------
    # 8. Log technical metadata only
    # --------------------------------------------------------

    print(
        json.dumps({
            "request_id": request_id,
            "event": webhook_event,
            "s3_key": s3_key,
            "body_size_bytes": len(raw_body),
            "status": "stored"
        })
    )

    # --------------------------------------------------------
    # 9. Return success
    # --------------------------------------------------------

    return response(
        200,
        "Webhook received"
    )