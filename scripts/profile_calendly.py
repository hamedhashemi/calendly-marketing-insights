
import json
import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET_NAME = "dea-calendly-marketing-hh"

S3_PREFIX = "landing/calendly/"

AWS_REGION = "us-east-1"

MAX_FILE_SIZE = 1024 * 1024  # 1 MB


# Calendly Marketing Event Types from project requirements

# ============================================================
# Confirmed Marketing Event Type Mapping
# ============================================================

MARKETING_EVENT_TYPES = {

    # Confirmed by Coach:
    # Breakthrough Session FB D2C Var

    "d5c9e359-c580-4c11-8bf5-531a3de5ae5c":
        "facebook_paid_ads"

}


# ============================================================
# 2. AWS CLIENT
# ============================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# 3. EXTRACT ID FROM CALENDLY URI
# ============================================================

def extract_id(uri):

    if not isinstance(uri, str):
        return None

    return uri.rstrip("/").split("/")[-1]


# ============================================================
# 4. CHECK WHETHER A RECORD IS A TEST
# ============================================================

def is_test_record(invitee_uri, event_uri):

    values = [
        invitee_uri or "",
        event_uri or ""
    ]

    return any(
        "test-invitee-" in value
        or "test-event-" in value
        for value in values
    )


# ============================================================
# 5. MAIN PROFILING FUNCTION
# ============================================================

def profile_calendly():

    print("=" * 65)
    print("CALENDLY LANDING DATA PROFILING")
    print("=" * 65)

    print(f"Bucket: {BUCKET_NAME}")
    print(f"Prefix: {S3_PREFIX}")
    print()

    # --------------------------------------------------------
    # Initialize counters
    # --------------------------------------------------------

    total_files = 0
    valid_json_files = 0

    invalid_json_files = 0
    skipped_large_files = 0
    test_files = 0

    duplicate_webhooks = 0
    duplicate_payloads = 0

    webhook_event_counts = Counter()

    event_type_counts = Counter()

    
    event_type_names = defaultdict(Counter)

    marketing_bookings = Counter()

    marketing_original_bookings = Counter()

    unique_booking_ids = set()
    unique_event_ids = set()

    seen_webhooks = set()
    seen_payload_hashes = set()

    rescheduled_bookings = 0
    original_bookings = 0

    group_event_ids = set()
    multi_employee_event_ids = set()

    employee_meetings = defaultdict(set)

    booking_dates = []

    data_quality_issues = Counter()

    # --------------------------------------------------------
    # List all objects using pagination
    # --------------------------------------------------------

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    pages = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX
    )

    # --------------------------------------------------------
    # Process files
    # --------------------------------------------------------

    for page in pages:

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.lower().endswith(".json"):
                continue

            total_files += 1

            # Avoid unexpectedly large objects

            if obj["Size"] > MAX_FILE_SIZE:

                skipped_large_files += 1
                continue

            try:

                response = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=key
                )

                raw_bytes = response["Body"].read()

                payload = json.loads(raw_bytes)

                valid_json_files += 1

            except (
                ClientError,
                ValueError,
                UnicodeDecodeError
            ):

                invalid_json_files += 1
                continue

            # ------------------------------------------------
            # Validate JSON structure
            # ------------------------------------------------

            if not isinstance(payload, dict):

                data_quality_issues[
                    "invalid_root_structure"
                ] += 1

                continue

            webhook_event = payload.get("event")

            data = payload.get("payload")

            if not isinstance(data, dict):

                data_quality_issues[
                    "missing_payload"
                ] += 1

                continue

            scheduled_event = data.get(
                "scheduled_event"
            ) or {}

            if not isinstance(scheduled_event, dict):

                data_quality_issues[
                    "invalid_scheduled_event"
                ] += 1

                continue

            invitee_uri = data.get("uri")

            event_uri = scheduled_event.get(
                "uri"
            ) or data.get("event")

            if not invitee_uri:

                data_quality_issues[
                    "missing_invitee_uri"
                ] += 1

                continue

            if not event_uri:

                data_quality_issues[
                    "missing_event_uri"
                ] += 1

                continue

            # ------------------------------------------------
            # Exclude known synthetic test records
            # ------------------------------------------------

            if is_test_record(
                invitee_uri,
                event_uri
            ):

                test_files += 1
                continue

            # ------------------------------------------------
            # Count identical payloads
            # ------------------------------------------------

            payload_hash = hashlib.sha256(
                raw_bytes
            ).hexdigest()

            if payload_hash in seen_payload_hashes:

                duplicate_payloads += 1

            else:

                seen_payload_hashes.add(
                    payload_hash
                )

            # ------------------------------------------------
            # Deduplicate by webhook event + invitee URI
            # ------------------------------------------------

            webhook_key = (
                webhook_event,
                invitee_uri
            )

            if webhook_key in seen_webhooks:

                duplicate_webhooks += 1
                continue

            seen_webhooks.add(
                webhook_key
            )

            webhook_event_counts[
                webhook_event
            ] += 1

            invitee_id = extract_id(
                invitee_uri
            )

            event_id = extract_id(
                event_uri
            )

            unique_booking_ids.add(
                invitee_id
            )

            unique_event_ids.add(
                event_id
            )

            # ------------------------------------------------
            # Event Type and Marketing Attribution
            # ------------------------------------------------

            event_type_uri = scheduled_event.get(
                "event_type"
            )

            event_type_id = extract_id(
                event_type_uri
            )

            if event_type_id:

                event_type_counts[
                    event_type_id
                ] += 1

            else:

                data_quality_issues[
                    "missing_event_type"
                ] += 1

            marketing_channel = (
                MARKETING_EVENT_TYPES.get(
                    event_type_id
                )
            )

            
            event_name = scheduled_event.get(
                    "name"
            )

            if event_name:

                    event_type_names[
                        event_type_id
                    ][event_name] += 1

            # ------------------------------------------------
            # Booking classification
            # ------------------------------------------------

            if webhook_event == "invitee.created":

                old_invitee = data.get(
                    "old_invitee"
                )

                if old_invitee:

                    rescheduled_bookings += 1

                else:

                    original_bookings += 1

                if marketing_channel:

                    marketing_bookings[
                        marketing_channel
                    ] += 1

                    if not old_invitee:

                        marketing_original_bookings[
                            marketing_channel
                        ] += 1

                booking_created_at = data.get(
                    "created_at"
                )

                if booking_created_at:

                    booking_dates.append(
                        booking_created_at
                    )

                else:

                    data_quality_issues[
                        "missing_booking_timestamp"
                    ] += 1

            # ------------------------------------------------
            # Group Event detection
            # ------------------------------------------------

            counter = scheduled_event.get(
                "invitees_counter"
            ) or {}

            if not isinstance(counter, dict):
                counter = {}

            total_invitees = counter.get(
                "total"
            )

            if (
                isinstance(total_invitees, int)
                and total_invitees > 1
            ):

                group_event_ids.add(
                    event_id
                )

            # ------------------------------------------------
            # Employee meeting analysis
            # ------------------------------------------------

            memberships = scheduled_event.get(
                "event_memberships"
            ) or []

            if not isinstance(memberships, list):

                data_quality_issues[
                    "invalid_event_memberships"
                ] += 1

                memberships = []

            if len(memberships) > 1:

                multi_employee_event_ids.add(
                    event_id
                )

            for member in memberships:

                if not isinstance(member, dict):
                    continue

                employee_id = extract_id(
                    member.get("user")
                )

                if employee_id:

                    employee_meetings[
                        employee_id
                    ].add(event_id)

    # ========================================================
    # 6. PRINT RESULTS
    # ========================================================

    print("\nFILE STATISTICS")
    print("-" * 65)

    print("Total JSON files:", total_files)

    print("Valid JSON files:", valid_json_files)

    print("Invalid/read-error files:", invalid_json_files)

    print("Skipped large files:", skipped_large_files)

    print("Known synthetic test files:", test_files)

    print("Duplicate payloads:", duplicate_payloads)

    print("Duplicate webhook event keys:", duplicate_webhooks)

    print("\nWEBHOOK EVENTS")
    print("-" * 65)

    for event_type, count in webhook_event_counts.most_common():

        print(
            f"{event_type}: {count}"
        )

    print("\nBOOKING STATISTICS")
    print("-" * 65)

    print(
        "Unique invitee IDs:",
        len(unique_booking_ids)
    )

    print(
        "Unique scheduled event IDs:",
        len(unique_event_ids)
    )

    print(
        "Original invitee.created events:",
        original_bookings
    )

    print(
        "Rescheduled invitee.created events:",
        rescheduled_bookings
    )

    print(
        "Group scheduled events:",
        len(group_event_ids)
    )

    print(
        "Multi-employee scheduled events:",
        len(multi_employee_event_ids)
    )

    print("\nMARKETING BOOKING STATISTICS")
    print("-" * 65)

    for channel in MARKETING_EVENT_TYPES.values():

        print(
            f"{channel}: "
            f"all_created={marketing_bookings[channel]}, "
            f"original={marketing_original_bookings[channel]}"
        )

    print("\nEVENT TYPE DISTRIBUTION")
    print("-" * 65)

    print(
        "Distinct Event Types:",
        len(event_type_counts)
    )

    for event_type_id, count in event_type_counts.most_common():

        channel = MARKETING_EVENT_TYPES.get(
            event_type_id,
            "not_in_project_mapping"
        )

        print(
            f"{event_type_id} | "
            f"count={count} | "
            f"channel={channel}"
        )

    
    print("\nEVENT TYPE ID TO MEETING NAME MAPPING")
    print("-" * 65)

    for event_type_id, names in event_type_names.items():

        print(f"\nEvent Type ID: {event_type_id}")

        for event_name, count in names.most_common():

            print(
                f"  Meeting: {event_name} | "
                f"Count: {count}"
            )

    print("\nEMPLOYEE MEETING STATISTICS")
    print("-" * 65)

    print(
        "Distinct employee IDs:",
        len(employee_meetings)
    )

    print(
        "Employee-event relationships:",
        sum(
            len(events)
            for events in employee_meetings.values()
        )
    )

    print("\nBOOKING DATE RANGE")
    print("-" * 65)

    if booking_dates:

        print(
            "Earliest:",
            min(booking_dates)
        )

        print(
            "Latest:",
            max(booking_dates)
        )

    else:

        print(
            "No booking timestamps found"
        )

    print("\nDATA QUALITY ISSUES")
    print("-" * 65)

    if data_quality_issues:

        for issue, count in data_quality_issues.items():

            print(
                f"{issue}: {count}"
            )

    else:

        print(
            "No structural data quality issues detected"
        )

    print("\n" + "=" * 65)
    print("PROFILING COMPLETED")
    print("=" * 65)


# ============================================================
# 7. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        profile_calendly()

    except NoCredentialsError:

        print(
            "ERROR: AWS credentials are not configured."
        )

    except ClientError as error:

        error_code = error.response[
            "Error"
        ].get("Code")

        print(
            f"AWS ERROR: {error_code}"
        )