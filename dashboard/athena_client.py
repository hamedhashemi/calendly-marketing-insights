import time

import boto3
import pandas as pd


AWS_REGION = "us-east-1"

ATHENA_DATABASE = "calendly_marketing"

ATHENA_OUTPUT = (
    "s3://dea-calendly-marketing-hh/"
    "athena-results/"
)

ATHENA_WORKGROUP = "primary"


athena = boto3.client(
    "athena",
    region_name=AWS_REGION
)


def run_query(sql: str) -> pd.DataFrame:

    response = athena.start_query_execution(

        QueryString=sql,

        QueryExecutionContext={
            "Database": ATHENA_DATABASE
        },

        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT
        },

        WorkGroup=ATHENA_WORKGROUP
    )

    query_id = response[
        "QueryExecutionId"
    ]

    # --------------------------------------------------------
    # Wait for query completion
    # --------------------------------------------------------

    while True:

        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        status = execution[
            "QueryExecution"
        ][
            "Status"
        ][
            "State"
        ]

        if status == "SUCCEEDED":
            break

        if status in (
            "FAILED",
            "CANCELLED"
        ):

            reason = execution[
                "QueryExecution"
            ][
                "Status"
            ].get(
                "StateChangeReason",
                "Unknown Athena error"
            )

            raise RuntimeError(
                f"Athena query {status}: {reason}"
            )

        time.sleep(1)

    # --------------------------------------------------------
    # Read paginated result
    # --------------------------------------------------------

    rows = []

    columns = None

    next_token = None

    first_page = True

    while True:

        request = {
            "QueryExecutionId": query_id,
            "MaxResults": 1000
        }

        if next_token:
            request[
                "NextToken"
            ] = next_token

        result = athena.get_query_results(
            **request
        )

        if columns is None:

            columns = [

                item["Name"]

                for item in result[
                    "ResultSet"
                ][
                    "ResultSetMetadata"
                ][
                    "ColumnInfo"
                ]

            ]

        page_rows = result[
            "ResultSet"
        ][
            "Rows"
        ]

        # First Athena row contains column headers.

        start_index = (
            1 if first_page else 0
        )

        for row in page_rows[
            start_index:
        ]:

            values = [

                item.get(
                    "VarCharValue"
                )

                for item in row.get(
                    "Data",
                    []
                )

            ]

            # Athena may omit trailing NULL fields.

            values += [
                None
            ] * (
                len(columns)
                - len(values)
            )

            rows.append(
                values
            )

        first_page = False

        next_token = result.get(
            "NextToken"
        )

        if not next_token:
            break

    return pd.DataFrame(
        rows,
        columns=columns
    )