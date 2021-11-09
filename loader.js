const AWS = require("aws-sdk");
const util = require("util");
const {v4:uuidv4} = require("uuid");

const MAX_ITERATION = 15;
const MAX_MESSAGE_COUNT = 30;

const chunks = require('array.chunk');
const RS_DATA_USER = "redshift_data_api_user";

exports.handler = async (event) => {
    const queueUrl = event.input.sqs_queue_url;
    const manifestBucket = event.output.manifest_bucket;
    const manifestPrefix = event.output.manifest_prefix;
    const copyOptions = event.copy.options;
    const copyRoleArn = event.copy.role_arn;
    const copyTableName = event.copy.table_name;
    const clusterIdentifier = event.cluster.identifier;
    const clusterDb = event.cluster.db_name;
    const mergeDuplicatePks = event.options.merge_duplicate_pks;
    const mergePk = event.options.merge_pk;
    const mergeTimestamp = event.options.merge_timestamp;

    const sqs = new AWS.SQS();
    var currentIteration = 0;
    var messages = [];

    while (currentIteration <= MAX_ITERATION) {
        var queueData = await sqs.receiveMessage({
            QueueUrl: queueUrl,
            AttributeNames: ["All"],
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 20
        }).promise();

        var payload = queueData.Messages;

        if (payload != null && payload.length > 0) {
            messages = messages.concat(payload);

            if (messages.length >= MAX_MESSAGE_COUNT) {
                break;
            }

            currentIteration++;
        } else {
            break;
        }
    }

    if (messages.length > 0) {
        const s3 = new AWS.S3();

        var manifest = {
            entries: []
        }

        var deleteMessageEntries = [];

        for (var i in messages) {
            const m = messages[i];
            const messageId = m.MessageId;
            const receiptHandle = m.ReceiptHandle;
            const body = JSON.parse(m.Body);
            
            if (body && body.Records && body.Records.length > 0) {
                const s3 = body.Records[0].s3;
                const bucketName = s3.bucket.name;
                const objectName = decodeURIComponent(s3.object.key);
    
                manifest.entries.push({
                    "url": "s3://"+bucketName+"/"+objectName,
                    "mandatory": true,
                    "meta": {
                        "content_length": s3.object.size
                    }
                });
            }

            deleteMessageEntries.push({
                Id: messageId,
                ReceiptHandle: receiptHandle
            })
        }

        const statementName = Date.now() + "_" + copyTableName;
        const manifestKey = manifestPrefix + uuidv4() + "_" + Date.now();
        const copyStagingTableName = copyTableName+"_"+Date.now();

        const generateManifestParams = {
            Body: JSON.stringify(manifest),
            Bucket: manifestBucket,
            Key: manifestKey,
        }

        await s3.putObject(generateManifestParams).promise();

        const rsData = new AWS.RedshiftData();
        var sqls = [
            util.format("CREATE TABLE %s as select * from %s where 1=2", copyStagingTableName, copyTableName),
            util.format("COPY %s from 's3://%s/%s' iam_role '%s' format as parquet manifest %s", copyStagingTableName, manifestBucket, manifestKey, copyRoleArn, copyOptions),
        ];

        if (mergeDuplicatePks) {
            sqls.push(util.format("DELETE from %s using %s where %s.%s=%s.%s", copyTableName, copyStagingTableName, copyTableName, mergePk, copyStagingTableName, mergePk));
        } else {
            sqls.push(util.format("DELETE from %s using %s where %s.%s=%s.%s and %s.%s=%s.%s", copyTableName, copyStagingTableName, copyTableName, mergePk, copyStagingTableName, mergePk, copyTableName, mergeTimestamp, copyStagingTableName, mergeTimestamp));
        }

        sqls.push(util.format("insert into %s with latest_rows as (select %s, max(%s) as latest_time from %s group by %s) select distinct o.* from %s o inner join latest_rows lr on o.%s=lr.%s where o.%s=lr.latest_time",
            copyTableName,
            mergePk,
            mergeTimestamp,
            copyStagingTableName,
            mergePk,
            copyStagingTableName,
            mergePk,
            mergePk,
            mergeTimestamp
        ));

        sqls.push(util.format("DROP TABLE %s", copyStagingTableName));

        const execResp = await rsData.batchExecuteStatement({
            ClusterIdentifier: clusterIdentifier,
            Database: clusterDb,
            Sqls: sqls,
            DbUser: RS_DATA_USER,
            StatementName: statementName,
            WithEvent: true
        }).promise();

        const deleteMessageEntryChunks = chunks(deleteMessageEntries, 10);

        for (i in deleteMessageEntryChunks) {
            const deleteMessageChunk = deleteMessageEntryChunks[i];
            await sqs.deleteMessageBatch({
                Entries: deleteMessageChunk,
                QueueUrl: queueUrl
            }).promise();
        }

        return {
            "statement_name": statementName,
            "statement_id": execResp.Id
        };
    }

    return {};
};
