# Solution Design Document
## Calendly Marketing Insights — AWS Data Engineering Pipeline

---

## 1. Document Purpose

This document describes the technical design of the Calendly Marketing Insights data engineering solution.

The system integrates Calendly booking events with daily marketing spend data and transforms the source data into analytics-ready datasets for marketing performance, booking behavior, and employee meeting workload.

The design covers:

- Source ingestion
- AWS architecture
- Data lake organization
- Delta Lake implementation
- Bronze, Silver, and Gold modeling
- Data quality
- Marketing attribution
- Workflow orchestration
- Scheduling
- Retry and reload behavior
- Failure notification
- Security
- Query and visualization
- CI/CD
- Known limitations
- Design decisions and rationale

---

# 2. Business Objective

The organization needs to understand how marketing activity translates into booked calls and how Calendly scheduling behavior affects operational workload.

The solution supports the following business questions:

1. How many calls are booked each day by marketing source?
2. What is the Cost Per Booking for each marketing channel?
3. How does booking volume change over time?
4. Which confirmed marketing sources generate bookings?
5. At what hours and on which weekdays are bookings most frequently created?
6. How many meetings are employees scheduled to handle each week?

The system therefore combines:

```text
Calendly Booking Data
        +
Marketing Spend Data
        =
Marketing and Operational Insights
```

---

# 3. Scope

## 3.1 In Scope

The implemented solution includes:

- Calendly webhook ingestion
- Marketing spend ingestion
- Amazon S3 Landing storage
- Delta Lake Bronze layer
- Delta Lake Silver layer
- Delta Lake Gold layer
- AWS Glue PySpark transformations
- Data quality checks
- Original booking vs reschedule classification
- Marketing attribution
- Cost Per Booking calculation
- Booking time analysis
- Employee meeting workload analysis
- AWS Glue Data Catalog
- Amazon Athena
- Streamlit dashboard
- AWS Step Functions orchestration
- EventBridge daily scheduling
- Retry / reload handling
- EventBridge failure detection
- SNS email notifications
- GitHub version control
- GitHub Actions CI/CD
- GitHub OIDC / AWS STS authentication

## 3.2 Out of Scope

The current implementation does not include:

- Predictive machine learning
- Paid-channel ROI beyond the available spend and booking data
- Verified meeting attendance
- Complete cancellation-state modeling
- Fully confirmed YouTube and TikTok Calendly mappings
- Hosted enterprise authentication for Streamlit
- Cryptographic verification of Calendly webhook signatures

These can be implemented as future enhancements.

---

# 4. Source Systems

## 4.1 Calendly

Calendly provides booking activity through organization-level webhooks.

Primary event currently processed:

```text
invitee.created
```

Relevant webhook information includes:

- Invitee URI
- Scheduled event URI
- Event Type URI
- Event name
- Booking creation timestamp
- Meeting start time
- Meeting end time
- Invitee status
- Invitee timezone
- UTM source
- UTM campaign
- Rescheduling relationships
- Employee event memberships
- Invitee count

Calendly events arrive continuously and therefore use an event-driven ingestion pattern.

---

## 4.2 Marketing Spend

Marketing spend data is published as JSON files in a provided public S3 location.

Source files follow a pattern similar to:

```text
spend_data_YYYY-MM-DD.json
```

The source also provides:

```text
file_index.json
```

The index is used by the ingestion logic to dynamically discover available files.

Each record contains:

```text
date
channel
spend
```

Supported source channels are:

```text
facebook_paid_ads
youtube_paid_ads
tiktok_paid_ads
```

Spend data is treated as USD.

The source is expected to publish Day-1 spend data daily.

---

# 5. High-Level Architecture

```text
                               +------------------+
                               |     Calendly     |
                               +---------+--------+
                                         |
                                      Webhook
                                         |
                                         v
                               +------------------+
                               |   API Gateway    |
                               +---------+--------+
                                         |
                                         v
                               +------------------+
                               | AWS Lambda       |
                               | Webhook Ingest   |
                               +---------+--------+
                                         |
                                         v
                               +------------------+
                               | S3 Landing       |
                               | Calendly         |
                               +---------+--------+
                                         |
                                         |
+----------------------+                 |
| Public Marketing     |                 |
| Spend S3 Source      |                 |
+----------+-----------+                 |
           |                             |
           v                             |
+----------------------+                 |
| AWS Lambda           |                 |
| Spend Ingestion      |                 |
+----------+-----------+                 |
           |                             |
           v                             |
+----------------------+                 |
| S3 Landing           |                 |
| Marketing Spend      |                 |
+----------+-----------+                 |
           |                             |
           +-------------+---------------+
                         |
                         v
                 +---------------+
                 | Delta Bronze  |
                 +-------+-------+
                         |
                         v
                 +---------------+
                 | Delta Silver  |
                 +-------+-------+
                         |
                         v
                 +---------------+
                 | Delta Gold    |
                 +-------+-------+
                         |
                         v
                 +---------------+
                 | Glue Catalog  |
                 +-------+-------+
                         |
                         v
                 +---------------+
                 | Amazon Athena |
                 +-------+-------+
                         |
                         v
                 +---------------+
                 |   Streamlit   |
                 +---------------+
```

---

# 6. Operational Architecture

The data pipeline is orchestrated independently from code deployment.

```text
EventBridge Scheduler
         |
         v
AWS Step Functions
         |
         +-------------------------+
         |                         |
         v                         v
Calendly ETL Branch        Marketing Spend ETL Branch
         |                         |
Landing -> Bronze          Landing -> Bronze
         |                         |
Bronze -> Silver           Bronze -> Silver
         |                         |
         +------------+------------+
                      |
                      v
                 Gold Outputs
                /            \
               v              v
       Marketing Gold    Operational Gold
```

Failure handling:

```text
Step Functions
      |
      | FAILED / TIMED_OUT / ABORTED
      v
EventBridge Rule
      |
      v
Amazon SNS
      |
      v
Email Notification
```

---

# 7. AWS Services and Design Rationale

## 7.1 Amazon API Gateway

### Purpose

Provides the public HTTP endpoint used by Calendly to send webhook requests.

### Why It Was Selected

API Gateway removes the need to operate a public web server.

It provides a managed integration point between an external SaaS system and AWS Lambda.

It is appropriate because the ingestion pattern is:

```text
External HTTP POST
       ->
Managed API Endpoint
       ->
Serverless Processing
```

---

## 7.2 AWS Lambda

### Purpose

Two Lambda functions are used:

```text
calendly-webhook-ingestion
marketing-spend-ingestion
```

### Why It Was Selected

Both ingestion workloads are lightweight and event-driven.

Running persistent EC2 instances or Spark jobs simply to receive small webhook events or retrieve a small daily JSON source would create unnecessary cost and operational complexity.

Lambda provides:

- Serverless execution
- Automatic scaling
- Native integration with API Gateway
- Native integration with Step Functions
- IAM role-based security
- Pay-per-use execution

---

## 7.3 Amazon S3

### Purpose

S3 is the central storage layer for the data lake.

### Why It Was Selected

S3 provides:

- Durable object storage
- Low cost
- Elastic scale
- Integration with Glue
- Integration with Athena
- Integration with Delta Lake
- Independent compute and storage

The architecture therefore does not require a persistent database server for raw and analytical data storage.

---

## 7.4 Delta Lake

### Purpose

Delta Lake provides transactional table behavior on top of S3.

### Why It Was Selected

Plain S3 objects alone do not provide table-level ACID transaction semantics.

Delta Lake adds:

- Transaction logs
- ACID writes
- MERGE support
- Schema control
- History
- Repeatable processing
- More reliable incremental updates

This is especially important for Silver datasets where repeat ingestion or updated records need to be merged safely.

---

## 7.5 AWS Glue

### Purpose

AWS Glue runs the main ETL transformations.

Glue jobs use PySpark and Delta Lake.

### Why It Was Selected

Glue provides managed Spark compute without requiring a persistent EMR cluster.

It is appropriate for:

- Nested JSON processing
- Distributed transformation
- Delta table operations
- Deduplication
- Joins
- Aggregations
- Data quality validation

The current project uses AWS Glue 5.0.

---

## 7.6 AWS Glue Data Catalog

### Purpose

Stores metadata about the Silver and Gold Delta tables.

### Why It Was Selected

The catalog separates logical table definitions from physical S3 storage paths.

Athena can therefore query business datasets by table name.

Example:

```text
calendly_marketing.gold_daily_marketing_performance
```

instead of directly querying S3 objects.

---

## 7.7 Amazon Athena

### Purpose

Provides the SQL query layer used by Streamlit.

### Why It Was Selected

The project does not currently require a dedicated data warehouse cluster.

Athena provides:

- Serverless SQL
- Direct S3 access
- Glue Catalog integration
- Low infrastructure overhead

For the current data volume, introducing Redshift would add infrastructure that is not necessary.

---

## 7.8 AWS Step Functions

### Purpose

Coordinates Lambda and Glue processing.

### Why It Was Selected

The pipeline contains dependencies and parallel branches.

Step Functions provides:

- Workflow state visibility
- Retry handling
- Parallel execution
- Execution history
- Clear dependency management
- Failure propagation

This makes the data pipeline easier to operate and troubleshoot than an ad hoc sequence of independent scheduled jobs.

---

## 7.9 Amazon EventBridge Scheduler

### Purpose

Starts the production workflow automatically every day.

Current schedule:

```text
07:15 AM
America/New_York
```

### Why It Was Selected

The Marketing Spend source is published daily.

EventBridge Scheduler provides timezone-aware scheduling and integrates directly with Step Functions.

The workflow is intentionally scheduled after the expected source publication time to provide a buffer.

---

## 7.10 Amazon EventBridge

### Purpose

Detects failed Step Functions executions.

### Why It Was Selected

Step Functions publishes execution state changes as AWS events.

EventBridge allows operational failures to be routed without adding notification logic inside ETL code.

Statuses monitored include:

```text
FAILED
TIMED_OUT
ABORTED
```

---

## 7.11 Amazon SNS

### Purpose

Delivers pipeline failure notifications by email.

### Why It Was Selected

SNS is a lightweight managed notification mechanism and integrates directly with EventBridge.

---

## 7.12 Streamlit

### Purpose

Provides interactive analytical visualization.

### Why It Was Selected

Streamlit allows a Python-based analytics dashboard to be created quickly without introducing a separate frontend framework.

The dashboard queries Gold data through Athena.

Core business transformations are intentionally not performed inside Streamlit.

---

## 7.13 GitHub Actions

### Purpose

Provides Continuous Integration and Continuous Deployment.

### Why It Was Selected

The source code is stored in GitHub, making GitHub Actions a natural automation layer for validation and deployment.

CI and CD remain separate from operational ETL execution.

---

## 7.14 GitHub OIDC and AWS STS

### Purpose

Authenticates GitHub Actions to AWS.

### Why It Was Selected

Long-lived AWS Access Keys should not be stored in GitHub.

The implemented approach is:

```text
GitHub
   |
   v
OIDC Token
   |
   v
AWS STS
   |
   v
Temporary Credentials
```

This provides short-lived credentials through:

```text
github-calendly-deploy-role
```

The role is restricted to the AWS resources required for deployment.

---

# 8. S3 Data Lake Design

The bucket used by the solution is:

```text
dea-calendly-marketing-hh
```

The logical structure follows:

```text
landing/
bronze/
silver/
gold/
scripts/
athena-results/
```

---

# 9. Landing Layer Design

## 9.1 Calendly Landing

Path:

```text
s3://dea-calendly-marketing-hh/landing/calendly/
```

Purpose:

- Preserve webhook payloads
- Maintain source lineage
- Allow replay
- Support troubleshooting
- Retain source history

The webhook ingestion Lambda writes raw JSON payloads into S3.

The Landing layer is intentionally kept close to the original source structure.

---

## 9.2 Marketing Spend Landing

Path:

```text
s3://dea-calendly-marketing-hh/landing/marketing_spend/
```

Files are stored using content-based versioning.

Example:

```text
source_file=spend_data_YYYY-MM-DD.json/
sha256=<content-hash>.json
```

Advantages:

- Duplicate downloads do not create duplicate versions
- Changed source content is retained
- Historical source versions can be traced
- Reload behavior is safe

---

# 10. Bronze Layer Design

## 10.1 Calendly Bronze

Path:

```text
s3://dea-calendly-marketing-hh/bronze/calendly/webhook_events/
```

Grain:

```text
one row per Landing source object
```

Important columns include:

```text
source_path
source_etag
source_size_bytes
source_modified_at
webhook_event_type
raw_json
payload_hash
json_valid
ingested_at
```

### Design Principle

Bronze preserves source data rather than prematurely imposing business rules.

---

## 10.2 Marketing Spend Bronze

Path:

```text
s3://dea-calendly-marketing-hh/bronze/marketing_spend/
```

Grain:

```text
one row per retrieved source-file content version
```

Bronze preserves:

- Source file
- Source date
- Raw JSON
- Hash
- File metadata
- Retrieval metadata
- Validation status

---

# 11. Silver Layer Design

## 11.1 Calendly Silver

Path:

```text
s3://dea-calendly-marketing-hh/silver/calendly/booking/
```

Grain:

```text
one row per unique invitee booking
```

Business key:

```text
invitee_id
```

Main transformations:

- Filter `invitee.created`
- Flatten nested webhook JSON
- Extract IDs from Calendly URIs
- Parse timestamps
- Validate required fields
- Exclude synthetic test events
- Identify original bookings
- Identify rescheduled bookings
- Extract employee membership
- Extract UTM attributes
- Apply confirmed marketing attribution
- Deduplicate
- Merge into Delta

---

# 12. Original Booking vs Reschedule

Calendly creates a new invitee record during rescheduling.

The pipeline therefore does not rely only on a generic `rescheduled` flag.

The important relationship is:

```text
old_invitee
```

Rule:

```text
old_invitee is null
       ->
is_original_booking = true
```

and:

```text
old_invitee exists
       ->
is_original_booking = false
```

This prevents rescheduled appointments from being counted as new marketing acquisitions.

---

# 13. Marketing Attribution Design

The project requirement originally provides Event Type IDs for Facebook, YouTube, and TikTok marketing campaigns.

During live source profiling, those IDs were not present in the received organization webhook data.

The implemented pipeline therefore does not silently assume that similarly named events belong to those channels.

The active mapping explicitly confirmed for the implementation is:

```text
Event:
Data Engineer Academy Breakthrough Session FB D2C Var

Event Type ID:
d5c9e359-c580-4c11-8bf5-531a3de5ae5c

Channel:
facebook_paid_ads
```

Current mapping status:

```text
facebook_paid_ads -> confirmed
youtube_paid_ads  -> pending
tiktok_paid_ads   -> pending
```

This conservative approach prevents incorrect CPB calculations.

---

# 14. Marketing Spend Silver

Path:

```text
s3://dea-calendly-marketing-hh/silver/marketing_spend/
```

Grain:

```text
spend_date + channel
```

The source contains overlapping rolling snapshots.

Therefore when multiple source versions contain the same business key, the Silver transformation selects the newest valid record.

Ranking priority:

```text
1. Newest source_file_date
2. Newest source_modified_at
3. source_path as deterministic tie-breaker
```

Delta MERGE is performed using:

```text
spend_date + channel
```

---

# 15. Gold Layer Design

Three analytics-ready datasets are currently generated.

---

## 15.1 Daily Marketing Performance

Path:

```text
s3://dea-calendly-marketing-hh/gold/daily_marketing_performance/
```

Grain:

```text
report_date + channel
```

Contains:

- Marketing spend
- Mapping status
- Original bookings
- Rescheduled bookings
- All created bookings
- Data coverage status
- Spend availability
- CPB status
- Cost Per Booking

### CPB Formula

```text
CPB =
Total Spend / Original Bookings
```

CPB is generated only when:

```text
mapping confirmed
AND
booking data complete
AND
spend exists
AND
original bookings > 0
```

Possible statuses:

```text
ready
mapping_pending
booking_data_incomplete
spend_missing
zero_original_bookings
```

---

## 15.2 Booking Time Analysis

Path:

```text
s3://dea-calendly-marketing-hh/gold/booking_time_analysis/
```

Purpose:

- Analyze booking creation hour
- Analyze weekday behavior
- Build booking trends
- Support heatmaps

Business timezone:

```text
America/New_York
```

This is important because a timestamp occurring after midnight UTC can still belong to the previous business day in Eastern Time.

---

## 15.3 Employee Meeting Load

Path:

```text
s3://dea-calendly-marketing-hh/gold/employee_meeting_load/
```

Correct meeting grain:

```text
employee_id + event_id
```

This prevents group events from being counted once per invitee.

Example:

```text
Five Invitees
      |
      v
One Scheduled Event
      |
      v
One Employee Meeting
```

Metrics include:

- Meetings per employee per week
- Total meetings
- Observed weeks
- Average meetings per week

The metric represents scheduled workload, not verified attendance.

---

# 16. Gold Recalculation Strategy

Gold tables are fully derived from Silver datasets.

Because current Gold datasets are relatively small, deterministic recomputation is used.

Advantages:

- Simpler business logic
- Easier recovery
- Easier reconciliation
- Less incremental-state complexity

Delta transaction history still provides table-level write history.

---

# 17. Data Quality Strategy

Data quality checks are embedded throughout the pipeline.

Examples include:

### Landing / Bronze

- Valid JSON
- File size limits
- Source file integrity
- Changed source object detection
- Content hashing

### Calendly Silver

- Required invitee ID
- Required event ID
- Required Event Type ID
- Required booking timestamp
- Duplicate invitee detection
- Synthetic test-record exclusion
- Employee membership inspection

### Marketing Spend Silver

- Valid date
- Valid channel
- Non-null spend
- Valid source records
- Duplicate date/channel detection

### Gold

- Duplicate business-key detection
- CPB consistency
- Booking reconciliation
- Employee workload validation

Critical violations raise exceptions.

The pipeline does not intentionally publish invalid data while reporting a successful ETL run.

---

# 18. Idempotency Design

The pipeline is designed so that a retry does not create duplicate business records.

## Marketing Spend Landing

Idempotency:

```text
SHA-256 content hash
```

## Calendly Bronze

Idempotency:

```text
source_path
```

## Calendly Silver

Idempotency:

```text
invitee_id
```

through Delta MERGE.

## Marketing Spend Silver

Idempotency:

```text
spend_date + channel
```

through Delta MERGE.

## Gold

Idempotency:

```text
deterministic recomputation
```

This is essential because Step Functions may retry failed tasks.

---

# 19. Workflow Design

State Machine:

```text
calendly-marketing-daily-pipeline
```

Conceptual execution:

```text
Start
  |
  v
Marketing Spend Ingestion Lambda
  |
  v
Parallel
  |
  +-------------------------------+
  |                               |
  v                               v
Calendly Landing -> Bronze    Spend Landing -> Bronze
  |                               |
  v                               v
Calendly Bronze -> Silver     Spend Bronze -> Silver
  |                               |
  +---------------+---------------+
                  |
                  v
             Parallel Gold
             /           \
            v             v
     Marketing Gold   Operational Gold
             \           /
              \         /
               v       v
                Success
```

---

# 20. Retry Strategy

Step Functions provides retry handling.

Lambda ingestion tasks use retry behavior with increasing delay.

Glue tasks are also retried when execution fails.

Retry is safe because downstream storage layers are designed to be idempotent.

A retry therefore does not automatically imply duplicated business data.

---

# 21. Scheduling Design

EventBridge Scheduler:

```text
calendly-marketing-daily-schedule
```

Schedule:

```text
07:15 AM Eastern
```

Timezone:

```text
America/New_York
```

The marketing spend source is expected earlier in the morning.

The additional time buffer reduces the likelihood of querying the source before Day-1 data becomes available.

---

# 22. Failure Notification

Step Functions execution state changes are monitored through EventBridge.

Statuses:

```text
FAILED
TIMED_OUT
ABORTED
```

Flow:

```text
Step Functions
      |
      v
EventBridge
      |
      v
SNS
      |
      v
Email
```

SNS email delivery was tested successfully.

---

# 23. Security Design

## 23.1 S3

S3 provides server-side encryption for stored objects.

The final implementation documentation should record the bucket's configured default encryption mode.

Data access is controlled through IAM.

---

## 23.2 Encryption in Transit

External and AWS service communication uses HTTPS/TLS.

Examples:

```text
Calendly -> API Gateway
Lambda -> AWS APIs
Streamlit -> Athena API
GitHub -> AWS OIDC/STS
```

---

## 23.3 IAM

IAM roles are used for:

- Calendly Lambda
- Marketing Spend Lambda
- AWS Glue
- Step Functions
- EventBridge Scheduler
- GitHub deployment

Credentials are not embedded in application source code.

---

## 23.4 GitHub Authentication

GitHub Actions assumes:

```text
github-calendly-deploy-role
```

using:

```text
GitHub OIDC
       ->
AWS STS
       ->
Temporary Credentials
```

No permanent AWS Access Key is required in GitHub Secrets.

---

## 23.5 PII

Calendly webhook data can contain personally identifiable information.

Potential PII includes:

- Name
- Email
- Phone number
- Meeting details

Design principles:

```text
Raw data remains private
        |
        v
Silver minimizes unnecessary PII
        |
        v
Gold contains analytical metrics
        |
        v
Dashboard consumes Gold
```

Raw webhook payloads are not committed to GitHub.

---

# 24. Webhook Security Limitation

The active Calendly webhook subscription was created without a signing key.

Therefore the current webhook implementation cannot cryptographically verify that each incoming POST request was signed by Calendly.

The system still performs:

- JSON validation
- Required-field validation
- Payload sizing controls
- Hash generation
- Controlled S3 storage

However, source authenticity is not cryptographically verified.

This is documented as a known security limitation rather than being hidden.

---

# 25. Athena Design

Glue Catalog database:

```text
calendly_marketing
```

Analytics tables include:

```text
silver_calendly_booking

silver_marketing_spend

gold_daily_marketing_performance

gold_booking_time_analysis

gold_employee_meeting_load
```

Athena queries the Delta tables directly.

Athena query results are stored under:

```text
s3://dea-calendly-marketing-hh/athena-results/
```

---

# 26. Dashboard Design

The Streamlit dashboard reads Gold datasets through Athena.

Major sections:

```text
Marketing & CPB
Booking Trends
Time Analysis
Employee Load
```

Visualizations include:

- KPI metrics
- Cost Per Booking charts
- Channel attribution table
- Daily booking trends
- Cumulative bookings
- Hour × weekday heatmap
- Booking volume by hour
- Booking volume by weekday
- Employee workload
- Weekly employee meeting trend

Business transformations remain in Gold rather than being duplicated inside Streamlit.

---

# 27. CI Design

Workflow:

```text
.github/workflows/ci.yml
```

Triggers:

```text
push -> main
pull_request -> main
```

CI checks:

- Required production files
- Python syntax
- Dashboard dependency installation
- Lambda handler existence

CI acts as a deployment gate.

---

# 28. CD Design

Workflow:

```text
.github/workflows/deploy.yml
```

CD is triggered only after successful CI on `main`.

Flow:

```text
Successful CI
      |
      v
Checkout exact validated commit
      |
      v
GitHub OIDC
      |
      v
AWS STS
      |
      v
Temporary AWS Credentials
      |
      +-----------------------+
      |                       |
      v                       v
Deploy Glue Scripts      Deploy Lambdas
```

Six Glue scripts are deployed to their existing production S3 locations.

Two Lambda functions are updated.

CD does not execute the production data pipeline.

---

# 29. Separation of Deployment and Execution

This distinction is intentional.

```text
GitHub CI/CD
     =
Deploy application and ETL code
```

while:

```text
EventBridge + Step Functions
     =
Execute production data processing
```

This prevents a developer code push from automatically starting business data processing.

---

# 30. Glue Production Scripts

Production scripts:

```text
calendly_landing_to_bronze.py

calendly_bronze_to_silver.py

marketing_spend_landing_to_bronze.py

marketing_spend_bronze_to_silver.py

calendly_marketing_silver_to_gold.py

calendly_operational_gold.py
```

Script locations:

```text
s3://dea-calendly-marketing-hh/scripts/
```

---

# 31. Key Business Rules

## Rule 1 — Reschedules Are Not New Acquisitions

```text
old_invitee exists
        ->
do not count as a new marketing booking
```

---

## Rule 2 — CPB Uses Original Bookings

```text
CPB =
Spend / Original Bookings
```

---

## Rule 3 — Aggregate CPB Is Recalculated

For multiple days:

```text
Aggregate CPB =
SUM(Spend) / SUM(Bookings)
```

Daily CPBs are not averaged.

---

## Rule 4 — Unknown Mapping Does Not Mean Zero Bookings

```text
Unconfirmed channel mapping
          !=
Zero bookings
```

Unknown attribution remains unknown.

---

## Rule 5 — Group Events Count Once per Employee

```text
multiple invitees
+
same event
+
same employee
=
one employee meeting
```

---

# 32. Current Limitations

## 32.1 YouTube and TikTok Mapping

Marketing spend exists for Facebook, YouTube, and TikTok.

Only the current Facebook Event Type mapping has been explicitly confirmed against live Calendly data.

Therefore YouTube and TikTok booking attribution remains pending.

---

## 32.2 UTM Availability

UTM parameters are often null.

The system retains them but cannot rely on them as the sole attribution mechanism.

---

## 32.3 Cancellation Modeling

The main Silver booking pipeline currently focuses on:

```text
invitee.created
```

Full `invitee.canceled` state modeling remains a potential enhancement.

---

## 32.4 Employee Attendance

The employee metric represents scheduled meeting workload.

It does not prove that a meeting occurred or that the employee attended.

---

## 32.5 Multi-Employee Events

Current profiling showed one employee membership per observed event.

The operational Gold logic detects unsupported multi-employee cases rather than silently undercounting them.

If multi-employee events appear, a normalized event-to-employee bridge should be added.

---

## 32.6 Webhook Signing

The current webhook subscription does not use a signing key.

Webhook authenticity therefore cannot currently be cryptographically verified.

---

# 33. Scalability Considerations

The design separates storage from compute.

S3 can scale independently of Glue.

Glue workers can be increased if transformation volume grows.

Step Functions can continue coordinating additional processing branches.

Potential future scale improvements include:

- S3 partition optimization
- Incremental Gold processing
- Dedicated event/employee bridge tables
- Additional Glue parallelism
- Athena scan optimization
- Additional observability metrics

The current design intentionally avoids unnecessary infrastructure for the existing workload.

---

# 34. Cost Considerations

The architecture primarily uses serverless or usage-based AWS services:

- Lambda
- Glue
- S3
- Athena
- Step Functions
- EventBridge
- SNS

This avoids paying continuously for idle servers.

Athena queries curated Gold data instead of raw webhook data, helping reduce unnecessary data scanning.

---

# 35. Testing Strategy

Testing was performed at multiple levels.

## Source Profiling

Calendly Landing data was profiled for:

- JSON validity
- Duplicate payloads
- Duplicate webhook keys
- Unique invitees
- Unique events
- Original bookings
- Reschedules
- Group events
- Multi-employee events
- Event Type distribution
- Employee-event relationships

---

## ETL Testing

Glue jobs were executed multiple times to validate repeatability.

Tests include:

- Duplicate-key checks
- Required-field checks
- Business-key uniqueness
- Channel validation
- Gold reconciliation
- CPB consistency
- Employee workload validation

---

## Orchestration Testing

The complete Step Functions workflow was manually executed successfully.

All workflow states completed successfully.

---

## Scheduler Testing

EventBridge Scheduler was configured and enabled.

---

## Notification Testing

SNS email notification was tested successfully.

---

## Dashboard Testing

The Streamlit dashboard was executed locally and successfully queried Athena.

---

## CI/CD Testing

GitHub CI executed successfully.

GitHub CD successfully authenticated to AWS using OIDC/STS and deployed production code.

---

# 36. Operational Runbook

## Daily Operation

Normally no manual action is required.

```text
07:15 Eastern
      |
      v
EventBridge Scheduler
      |
      v
Step Functions
      |
      v
Daily ETL
```

---

## If Pipeline Fails

1. Receive SNS email notification.
2. Open Step Functions execution history.
3. Identify failed state.
4. Inspect Glue or Lambda logs.
5. Correct the underlying problem.
6. Redrive or rerun the workflow.

Because the data layers are designed for idempotency, pipeline retries should not intentionally duplicate business data.

---

## If Dashboard Fails

Check:

```text
AWS credentials
Athena permissions
Athena query status
Glue Catalog tables
Gold table availability
Athena result location
```

---

# 37. Repository Structure

```text
calendly-marketing-insights/
|
|-- .github/
|   `-- workflows/
|       |-- ci.yml
|       `-- deploy.yml
|
|-- dashboard/
|   |-- app.py
|   |-- athena_client.py
|   `-- requirements.txt
|
|-- glue/
|   |-- calendly_landing_to_bronze.py
|   |-- calendly_bronze_to_silver.py
|   |-- marketing_spend_landing_to_bronze.py
|   |-- marketing_spend_bronze_to_silver.py
|   |-- calendly_marketing_silver_to_gold.py
|   `-- calendly_operational_gold.py
|
|-- lambda_calendly_webhook/
|   `-- lambda_function.py
|
|-- lambda_marketing_spend/
|   `-- lambda_function.py
|
|-- scripts/
|   |-- ingest_marketing_spend.py
|   `-- profile_calendly.py
|
|-- tests/
|   `-- test_calendly_webhook.py
|
|-- docs/
|   |-- architecture/
|   |-- screenshots/
|   `-- solution_design.md
|
|-- .gitignore
`-- README.md
```

---

# 38. Future Enhancements

Potential improvements include:

- Confirm YouTube Calendly mapping
- Confirm TikTok Calendly mapping
- Model `invitee.canceled`
- Improve campaign-level UTM attribution
- Add normalized event-to-employee Silver modeling
- Add authenticated hosted Streamlit access
- Add CloudWatch operational dashboarding
- Add additional automated tests
- Add pipeline cost monitoring
- Improve Athena partition optimization
- Add centralized data quality reporting

These enhancements are intentionally separate from the current project scope.

---

# 39. Design Outcome

The final solution provides a complete pipeline from external source ingestion through business visualization.

```text
Calendly + Marketing Spend
            |
            v
       AWS Ingestion
            |
            v
       S3 Landing
            |
            v
      Delta Bronze
            |
            v
      Delta Silver
            |
            v
       Delta Gold
            |
            v
      Glue Catalog
            |
            v
         Athena
            |
            v
        Streamlit
```

Operational execution is managed through:

```text
EventBridge
     |
     v
Step Functions
     |
     v
Lambda + Glue
```

Deployment is managed independently through:

```text
GitHub
   |
   v
CI
   |
   v
CD
   |
   v
OIDC / STS
   |
   v
AWS
```

The design emphasizes:

- Clear data lineage
- Separation of concerns
- Idempotent processing
- Data quality
- Serverless AWS services
- Explicit business rules
- Secure deployment
- Operational visibility
- Avoidance of unnecessary infrastructure

---

# 40. Implementation Status

The following components are implemented and successfully tested:

```text
[Completed] Calendly webhook endpoint
[Completed] API Gateway integration
[Completed] Calendly Lambda ingestion
[Completed] Marketing Spend Lambda ingestion
[Completed] S3 Landing storage
[Completed] Calendly Bronze Delta
[Completed] Marketing Spend Bronze Delta
[Completed] Calendly Silver Delta
[Completed] Marketing Spend Silver Delta
[Completed] Daily Marketing Performance Gold
[Completed] Booking Time Analysis Gold
[Completed] Employee Meeting Load Gold
[Completed] Glue Data Catalog
[Completed] Athena query layer
[Completed] Streamlit dashboard
[Completed] Step Functions workflow
[Completed] EventBridge Scheduler
[Completed] Retry / reload strategy
[Completed] EventBridge failure rule
[Completed] SNS notification
[Completed] GitHub repository
[Completed] GitHub CI
[Completed] GitHub CD
[Completed] GitHub OIDC
[Completed] AWS STS authentication
```

The implementation satisfies the core technical requirements of the Calendly Marketing Insights project while documenting the current source-data limitations explicitly.