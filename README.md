# Redshift Microbatch

The basic flow is S3 -> SQS (NewObject notification), the Lambda function would then be scheduled to run every few minutes (depending on data volume and processing SLAs) which would then read from SQS, generate the manifest file, then send the `COPY` command to Redshift via the Data API.

## Input Structure

The following is the input structure that the Lambda function expects:

```json
{
    "input": {
        "sqs_queue_url": "<SQS URL>"
    },
    "output": {
        "manifest_bucket": "<BUCKET_NAME>",
        "manifest_prefix": "<PREFIX_FOLDER_WHERE_TO_STORE_MANIFEST>"
    },
    "copy": {
        "options": "<COPY_OPTIONS_SPACE_SEPARATED>",
        "role_arn": "<COPY_IAM_ROLE_ARN>",
        "table_name": "<TARGET_TABLE_TO_WRITE>"
    },
    "cluster": {
        "identifier": "<CLUSTER_IDENTIFIER>",
        "db_name": "<CLUSTER_DB_NAME>"
    },
    "options": {
        "merge_duplicate_pks": true,
        "merge_pk": "<PK_COL_NAME>",
        "merge_timestamp": "<TIMESTAMP_COLUMN_FOR_DUPLICATE_PKS>"
    }
}
```