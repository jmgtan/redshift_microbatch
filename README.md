# Redshift Loader Utilities

Repository contains a number of utility Lambda functions that supports microbatching as well as bulk loading of data coming into S3. The functions are as follows:

- `functions/MicrobatchLoader/loader.js`: designed to be triggered directly from a SQS queue. The typical flow would be S3 -> SQS -> Lambda. Only once load job per table would execute, if more concurrent load job request for the same table gets triggered, it would be stored in DynamoDB as a pending item.
- `functions/NextLoader/next_loader.js`: Once a previous load job completes, the Redshift Data API would emit an event which would trigger this Lambda. The Lambda would check the DynamoDB table if there's any other pending load job for the same table and then execute it.
- `functions/BulkLoader/bulk_loader.js`: designed to be used separately and triggered either manually or automatically via scheduled job. Recommendation is to use this to load the previous day's data into Redshift.

## Deployment Using CDK
To use the CDK script, execute `npm run deploy`. This will deploy the following:

- IAM Role for the Lambda function
- DynamoDB Table
- S3 bucket for configuration as well as pending job metadata
- All the 3 lambda functions mentioned in the previous section.

After the deployment is done, you need to do the following:

- Add SQS as the trigger for the MicrobatchLoader Lambda function.
- Add the queue config json files (see the next section for more details) in the newly created S3 bucket from the CDK in the `rs-loader-config/` folder.

## Setting Up the SQS Triggered Function

### Overview
The basic flow of the setup are as follows:

1. Create a new SQS queue. As per the recommendation from the [AWS Lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#events-sqs-queueconfig), set the queue's visibility timeout to at least **6 times** the timeout of the Lambda function.
2. Configure S3 event notification to push NewObject events to the SQS queue.
3. Configure SQS as the Lambda trigger.

Each table that you want to load would have its own SQS queue. In order for the Lambda function to know the ETL configuration, create JSON configuration per queue that you intent to trigger the Lambda function. The JSON configuration should be named after the queue (eg. orders_queue.json) and uploaded into S3. The configuration structure are as follows:

```json
{
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

### Implication of Concurrent Loads
There would be instances where an execution of `loader.handler` would result in a `serializable isolation violation` error. This means that there's another transaction that is running the `DELETE` or `INSERT` statements. There are 3 ways to address this error, these are as follows:

1. Execute `LOCK tableName` at the start of the Lambda function. This would force all other ETL jobs for the same table to wait until the currently executing job completes. This would also impact the performance of all other users of the table which includes dashboards and reporting queries. This is the easiest way to fix this error, but has an impact on the performance and concurrency of Redshift.
2. The second approach is to just retry the job which sounds simple, but requires coordination and state management of which jobs to retry. This also means that the load job has run and failed and has to be retried until it succeeds.
3. The last approach is to only execute one job per table and manage the orchestration so that once a load job completes, the next in queue would execute. This is what the `next_loader.handler` function is doing.

The following is the rule that was used in EventBridge to trigger the `next_loader.handler` function:

```json
{
  "source": ["aws.redshift-data"],
  "detail-type": ["Redshift Data Statement Status Change"],
  "detail": {
    "state": ["FINISHED"],
    "statementName": [{
      "prefix": "<REDSHIFT_DB_NAME>/"
    }]
  }
}
```

Inside the Lambda function, the DynamoDB table is used to check PENDING jobs and the other pertinent information such as the location of the manifest file.

### DynamoDB Table
A DynamoDB table is used to keep track of pending jobs per table. The basic structure of the DynamoDB table are as follows:

- `db_table` (`String`, `Primary Key`)
- `status_timestamp` (`String`, `Sort Key`)

### Environment Variables
The following are the relevant environment variables per Lambda function:

- `loader.handler`
    - `CONFIG_BUCKET`: the bucket name where the JSON config files are stored. Just provide the bucket name, **DO NOT INCLUDE `s3://`**.
    - `CONFIG_PREFIX`: the folder where the JSON config files are stored. Example: `path/to/config/`.
    - `DDB_TRACKER`: the name of the DynamoDB table.
    - `PENDING_BUCKET`: the bucket name where pending metadata would be stored.
    - `PENDING_PREFIX`: the folder where the pending metadata would be stored. Example: `path/to/folder/`.
    - `MANIFEST_BUCKET`: the bucket name where manifest files would be stored.
    - `MANIFEST_PREFIX`: the folder where manifest files would be stored. Example: `path/to/folder/`.
- `next_loader.hander`
    - `DDB_TRACKER`: the name of the DynamoDB table.

### Lambda Timeouts
The following are the timeout configuration per Lambda function:

- `loader.handler`: 2 minutes
- `next_loader.hander`: 2 minutes

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

- `bulk_loader.handler`: 5 minutes