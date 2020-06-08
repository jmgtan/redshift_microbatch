const AWS = require("aws-sdk");
const {v4:uuidv4} = require("uuid");

const MAX_ITERATION = 15;
const MAX_MESSAGE_COUNT = 10;

const chunks = require('array.chunk');

exports.handler = async (event) => {
    const queueUrl = process.env.queue_url;
    const manifestBucket = process.env.manifest_bucket;
    const manifestPrefix = process.env.manifest_prefix;
    const copyOptions = process.env.copy_options;
    const copyRoleArn = process.env.copy_role_arn;
    const copyTableName = process.env.copy_table_name;
    const clusterIdentifier = process.env.cluster_identifier;
    const clusterUsername = process.env.cluster_username;
    const clusterDb = process.env.cluster_db;

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
        const rs = new AWS.Redshift();

        var manifest = {
            entries: []
        }

        var deleteMessageEntries = [];

        for (var i in messages) {
            const m = messages[i];
            const messageId = m.MessageId;
            const receiptHandle = m.ReceiptHandle;
            const body = JSON.parse(messages[i].Body);

            const s3 = body.Records[0].s3;
            const bucketName = s3.bucket.name;
            const objectName = s3.object.key;

            manifest.entries.push({
                "url": "s3://"+bucketName+"/"+objectName,
                "mandatory": true
            });

            deleteMessageEntries.push({
                Id: messageId,
                ReceiptHandle: receiptHandle
            })
        }

        const manifestKey = manifestPrefix + uuidv4()

        const generateManifestParams = {
            Body: JSON.stringify(manifest),
            Bucket: manifestBucket,
            Key: manifestKey,
        }

        await s3.putObject(generateManifestParams).promise();

        const clusterCredentials = await rs.getClusterCredentials({
            ClusterIdentifier: clusterIdentifier,
            DbUser: clusterUsername,
            AutoCreate: true,
            DbName: clusterDb
        }).promise();

        const clusterDetails = await rs.describeClusters({
            ClusterIdentifier: clusterIdentifier
        }).promise();

        if (clusterDetails != null && clusterDetails.Clusters.length > 0) {
            const endpoint = clusterDetails.Clusters[0].Endpoint;
            
            const { Client } = require('pg');

            const client = new Client({
                host: endpoint.Address,
                port: endpoint.Port,
                user: clusterCredentials.DbUser,
                password: clusterCredentials.DbPassword,
                database: clusterDb,
                ssl: true
            });

            await client.connect();

            const copyCommand = "COPY "+copyTableName+" from 's3://"+manifestBucket+"/"+manifestKey+"' iam_role '"+copyRoleArn+"' manifest "+copyOptions;

            await client.query(copyCommand);

            await client.end();

            const deleteMessageEntryChunks = chunks(deleteMessageEntries, 10);

            for (i in deleteMessageEntryChunks) {
                const deleteMessageChunk = deleteMessageEntryChunks[i];
                await sqs.deleteMessageBatch({
                    Entries: deleteMessageChunk,
                    QueueUrl: queueUrl
                }).promise();
            }
        }
    }

    return {};
};
