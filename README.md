# Redshift Loader Utilities

Repository contains a number of utility Lambda functions that supports microbatching as well as bulk loading of data coming into S3. The functions are as follows:

- `loader.handler`: designed to be triggered directly from a SQS queue. The typical flow would be S3 -> SQS -> Lambda. 
- `retry_microbatch.handler`: designed to allow retry if ever there's a load failure due to `serializable isolation violation` error. This function would be triggered by Amazon EventBridge as a result of a failed execution of the `loader.handler` function. This is a compromise instead of locking the table everytime the microbatch function runs which would compromise the concurrency performance of the table.
- `bulk_loader.handler`: designed to be used separately and triggered either manually or automatically via scheduled job. Recommendation is to use this to load the previous day's data into Redshift.

## Setting Up the SQS Triggered Function

### Overview
The basic flow of the setup are as follows:

1. Create a new SQS queue. As per the recommendation from the [AWS Lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#events-sqs-queueconfig), set the queue's visibility timeout to at least **6 times** the timeout of the Lambda function.
2. Configure S3 event notification to push NewObject events to the SQS queue.
3. Configure SQS as the Lambda trigger.

Each table that you want to load would have its own SQS queue. In order for the Lambda function to know the ETL configuration, create JSON configuration per queue that you intent to trigger the Lambda function. The JSON configuration should be named after the queue (eg. orders_queue.json) and uploaded into S3. The configuration structure are as follows:

```json
{
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

### EventBridge and Retrying
There would be instances where an execution of `loader.handler` would result in a `serializable isolation violation` error. This means that there's another transaction that is running the `DELETE` or `INSERT` statements. There are 2 ways to address this error, these are as follows:

1. Execute `LOCK tableName` at the start of the Lambda function. This would force all other ETL jobs for the same table to wait until the currently executing job completes. This would also impact the performance of all other users of the table which includes dashboards and reporting queries. This is the easiest way to fix this error, but has an impact on the performance and concurrency of Redshift.
2. The second approach is to just retry the job which sounds simple, but requires coordination and state management of which jobs to retry. This is where EventBridge comes in as the Redshift Data API supports emitting status changes to EventBridge when `WithEvent` is set to `true` which is the case in this implementation.

The following is the rule that was used in EventBridge to trigger the `retry_microbatch.handler` function:

```json
{
  "source": ["aws.redshift-data"],
  "detail-type": ["Redshift Data Statement Status Change"],
  "detail": {
    "state": ["FAILED"],
    "statementName": [{
      "prefix": "<REDSHIFT_DB_NAME>/"
    }]
  }
}
```

Inside the Lambda function, we do a further check that only queries that failed due to `serializable isolation violation` error gets retried. This would avoid infinite loops from happening.

### Environment Variables
The following are the relevant environment variables per Lambda function:

- `loader.handler`
    - `CONFIG_BUCKET`: the bucket name where the JSON config files are stored. Just provide the bucket name, **DO NOT INCLUDE `s3://`**.
    - `CONFIG_PREFIX`: the folder where the JSON config files are stored. Example: `path/to/config/`.
- `retry_loader.hander`
    - `COPY_IAM_ROLE_ARN`: the ARN of the Redshift IAM Role to be used for the `COPY` command.

### Lambda Timeouts
The following are the timeout configuration per Lambda function:

- `loader.handler`: 2 minutes
- `retry_loader.hander`: 2 minutes

## Bulk Loader

### Overview
The bulk loader function can be used to load partition specific datasets. 

### Bulk Loader Input Structure

The following is the input structure for the bulk loader:

```json
{
    "paths": [
        {
            "s3_path": "<S3_PATH>",
            "table": "<TARGET_TABLE>",
            "options": {
                "merge_duplicate_pks": true,
                "merge_pk": "<PK_COL_NAME>",
                "merge_timestamp": "<TIMESTAMP_COLUMN_FOR_DUPLICATE_PKS>"
            }
        }
    ],
    "copy": {
        "role_arn": "<COPY_IAM_ROLE_ARN>"
    },
    "cluster": {
        "identifier": "<CLUSTER_IDENTIFIER>",
        "db_name": "<CLUSTER_DB_NAME>"
    },
}
```

### Lambda Timeouts
The following are the timeout configuration per Lambda function:

- `retry_microbatch.handler`: 5 minutes