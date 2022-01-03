const AWS = require("aws-sdk");
const util = require("util");
const {v4:uuidv4} = require("uuid");
const {loadSQLBuilder, loadTracker} = require("/opt/nodejs/load_sql_builder");

const RS_DATA_USER = "redshift_data_api_user";

const manifestCache = {};
const s3 = new AWS.S3();
const rsData = new AWS.RedshiftData();
const collectedResponse = [];

const manifestBucket = process.env.MANIFEST_BUCKET;
var manifestPrefix = process.env.MANIFEST_PREFIX;

if (!manifestPrefix.endsWith("/")) {
    manifestPrefix += "/";
}

exports.handler = async (event) => {
    if (event.Records.length > 0) {
        
        for (var j in event.Records) {
            const eventRecord = event.Records[j];

            const eventSourceARN = eventRecord.eventSourceARN;

            if (eventSourceARN && eventSourceARN.indexOf("sqs") != -1) {
                if (!(eventSourceARN in manifestCache)) {
                    manifestCache[eventSourceARN] = {
                        entries: []
                    }
                }

                const body = JSON.parse(eventRecord.body);

                if (body && body.Records && body.Records.length > 0) {
                    const s3 = body.Records[0].s3;
                    const bucketName = s3.bucket.name;
                    const objectName = decodeURIComponent(s3.object.key);
        
                    manifestCache[eventSourceARN].entries.push({
                        "url": "s3://"+bucketName+"/"+objectName,
                        "mandatory": true,
                        "meta": {
                            "content_length": s3.object.size
                        }
                    });
                }
            } else {
                console.log(eventRecord);
            }
        }

        if (Object.keys(manifestCache).length > 0) {
            for (eventSourceARN in manifestCache) {
                const config = await loadSQLBuilder.getConfig(eventSourceARN);
                console.log(config);
                const copyOptions = config.copy.options;
                const copyRoleArn = config.copy.role_arn;
                const copyTableName = config.copy.table_name;
                const clusterIdentifier = config.cluster.identifier;
                const clusterDb = config.cluster.db_name;
                const mergeDuplicatePks = config.options.merge_duplicate_pks;
                const mergePk = config.options.merge_pk;
                const mergeTimestamp = config.options.merge_timestamp;

                const manifest = manifestCache[eventSourceARN];

                const manifestKey = manifestPrefix + copyTableName + "/" + uuidv4() + "_" + Date.now();
                const copyStagingTableName = copyTableName+"_"+Date.now();
                const statementName = clusterDb+"/"+copyStagingTableName;
        
                const generateManifestParams = {
                    Body: JSON.stringify(manifest),
                    Bucket: manifestBucket,
                    Key: manifestKey,
                }
        
                await s3.putObject(generateManifestParams).promise();
                
                try {
                    const manifestFiles = [{bucket: manifestBucket, key: manifestKey}];
                    await loadTracker.track(clusterDb, copyTableName, statementName);
                    console.log("Calling Pending Batch");
                    const pendingRecords = await loadTracker.trackAllPendingBatch(clusterDb, copyTableName);
                    if (pendingRecords && pendingRecords.length > 0) {
                        for (const pendingRecord of pendingRecords) {
                            manifestFiles.push({
                                bucket: pendingRecord.manifest_bucket.S,
                                key: pendingRecord.manifest_key.S
                            });
                        }
                    }
                    console.log("Pending Records");
                    console.log(pendingRecords);

                    const execResp = await rsData.batchExecuteStatement({
                        ClusterIdentifier: clusterIdentifier,
                        Database: clusterDb,
                        Sqls: loadSQLBuilder.generate(copyTableName, copyStagingTableName, mergeDuplicatePks, mergePk, mergeTimestamp, manifestFiles, copyRoleArn, copyOptions),
                        DbUser: RS_DATA_USER,
                        StatementName: statementName,
                        WithEvent: true
                    }).promise();
    
                    collectedResponse.push({
                        "table_name": copyTableName,
                        "statement_name": statementName,
                        "statement_id": execResp.Id,
                        "manifest_file": manifestKey
                    });
                } catch (e) {
                    await loadTracker.queue(clusterDb, copyTableName, statementName, manifestBucket, manifestKey, eventSourceARN);
                }
            }

            console.log(collectedResponse);

            return {
                "data_api_responses": collectedResponse
            };
        }
    }

    return {};
};
