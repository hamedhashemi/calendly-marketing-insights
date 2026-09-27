
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request

from datetime import datetime, timezone
from uuid import uuid4


# ============================================================
# 1. Configuration
# ============================================================

WEBHOOK_URL = (
    "https://86hhjflt9j.execute-api.us-east-1.amazonaws.com"
    "/webhook/calendly"
)

SIGNING_KEY = os.environ.get("WEBHOOK_SIGNING_KEY")

if not SIGNING_KEY:
    raise RuntimeError(
        "WEBHOOK_SIGNING_KEY environment variable is missing."
    )


# ============================================================
# 2. Create a sample Calendly webhook payload
# ============================================================

test_id = str(uuid4())

created_at = datetime.now(
    timezone.utc
).isoformat().replace("+00:00", "Z")

payload = {
    "event": "invitee.created",
    "created_at": created_at,
    "payload": {
        "uri": (
            "https://api.calendly.com/scheduled_events/"
            f"test-event-{test_id}/invitees/"
            f"test-invitee-{test_id}"
        ),
        "email": "test@example.com",
        "name": "Test Invitee",
        "status": "active",
        "created_at": created_at,
        "rescheduled": False,
        "scheduled_event": {
            "uri": (
                "https://api.calendly.com/scheduled_events/"
                f"test-event-{test_id}"
            ),
            "name": "Data Engineer Academy Info Session",
            "event_type": (
                "https://api.calendly.com/event_types/"
                "d639ecd3-8718-4068-955a-436b10d72c78"
            ),
            "start_time": "2026-09-20T18:00:00Z",
            "end_time": "2026-09-20T18:15:00Z",
            "status": "active"
        },
        "tracking": {
            "utm_source": None,
            "utm_medium": None,
            "utm_campaign": None,
            "utm_content": None,
            "utm_term": None
        }
    }
}


# ============================================================
# 3. Serialize JSON
# ============================================================

raw_body = json.dumps(
    payload,
    separators=(",", ":"),
    ensure_ascii=False
).encode("utf-8")


# ============================================================
# 4. Generate Calendly-compatible HMAC signature
# ============================================================

timestamp = str(int(time.time()))

signed_payload = (
    timestamp.encode("utf-8")
    + b"."
    + raw_body
)

signature = hmac.new(
    SIGNING_KEY.encode("utf-8"),
    signed_payload,
    hashlib.sha256
).hexdigest()

signature_header = f"t={timestamp},v1={signature}"


# ============================================================
# 5. Prepare HTTP POST request
# ============================================================

request = urllib.request.Request(
    WEBHOOK_URL,
    data=raw_body,
    headers={
        "Content-Type": "application/json",
        "Calendly-Webhook-Signature": signature_header
    },
    method="POST"
)


# ============================================================
# 6. Send request and display response
# ============================================================

print("=" * 60)
print("Calendly Webhook Integration Test")
print("=" * 60)

print(f"Test ID: {test_id}")
print(f"Endpoint: {WEBHOOK_URL}")
print(f"Event Type: {payload['event']}")
print("-" * 60)

try:
    with urllib.request.urlopen(
        request,
        timeout=15
    ) as response:

        response_body = response.read().decode("utf-8")

        print(f"HTTP Status: {response.status}")
        print(f"Response: {response_body}")

        if response.status == 200:
            print("\nSUCCESS: Webhook accepted by Lambda.")
            print("Next step: Verify the JSON file in S3.")

except urllib.error.HTTPError as error:

    error_body = error.read().decode("utf-8")

    print(f"HTTP Status: {error.code}")
    print(f"Response: {error_body}")

    if error.code == 401:
        print(
            "\nERROR: Invalid webhook signature. "
            "Verify that the local SIGNING_KEY matches "
            "the WEBHOOK_SIGNING_KEY in AWS Lambda."
        )

    elif error.code == 404:
        print(
            "\nERROR: API route not found. "
            "Check the API Gateway URL and POST route."
        )

    elif error.code in (500, 502):
        print(
            "\nERROR: Server-side failure. "
            "Check AWS Lambda CloudWatch logs and "
            "API Gateway integration permissions."
        )

except urllib.error.URLError as error:

    print(f"Connection Error: {error.reason}")

except TimeoutError:

    print("ERROR: Request timed out.")

print("=" * 60)