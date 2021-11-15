const AWS = require("aws-sdk");
const util = require("util");
const {v4:uuidv4} = require("uuid");

const RS_DATA_USER = "redshift_data_api_user";
const WAIT_TIME = 30000;
const configCache = {};
const manifestCache = {};
const s3 = new AWS.S3();
const rsData = new AWS.RedshiftData();
const collectedResponse = [];

var configBucket = process.env.CONFIG_BUCKET;
var configPrefix = process.env.CONFIG_PREFIX;

const loadConfigForQueue = async (eventSourceARN) => {
    const queue = eventSourceARN.substr(eventSourceARN.lastIndexOf(":") + 1);

    if (!(queue in configCache)) {

        if (!configPrefix.endsWith("/")) {
            configPrefix += "/";
        }

        const configKey = configPrefix + queue + ".json";
        const configResponse = await s3.getObject({Bucket: configBucket, Key: configKey}).promise();
        const config = JSON.parse(configResponse.Body.toString());

        configCache[queue] = config;
    }

    return configCache[queue];
}

const sleep = async (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    })
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
            await sleep(WAIT_TIME);
            for (eventSourceARN in manifestCache) {
                const config = await loadConfigForQueue(eventSourceARN);

                const manifestBucket = config.output.manifest_bucket;
                const manifestPrefix = config.output.manifest_prefix;
                const copyOptions = config.copy.options;
                const copyRoleArn = config.copy.role_arn;
                const copyTableName = config.copy.table_name;
                const clusterIdentifier = config.cluster.identifier;
                const clusterDb = config.cluster.db_name;
                const mergeDuplicatePks = config.options.merge_duplicate_pks;
                const mergePk = config.options.merge_pk;
                const mergeTimestamp = config.options.merge_timestamp;

                const manifest = manifestCache[eventSourceARN];

                const manifestKey = manifestPrefix + uuidv4() + "_" + Date.now();
                const copyStagingTableName = copyTableName+"_"+Date.now();
                const statementName = clusterDb+"/"+copyStagingTableName;
        
                const generateManifestParams = {
                    Body: JSON.stringify(manifest),
                    Bucket: manifestBucket,
                    Key: manifestKey,
                }
        
                await s3.putObject(generateManifestParams).promise();
        
                var sqls = [
                    util.format("CREATE TEMP TABLE %s (like %s)", copyStagingTableName, copyTableName),
                    util.format("COPY %s from 's3://%s/%s' iam_role '%s' format as parquet manifest %s", copyStagingTableName, manifestBucket, manifestKey, copyRoleArn, copyOptions)
                ];
        
                if (mergeDuplicatePks) {
                    sqls.push(util.format("DELETE from %s using %s where %s.%s=%s.%s and %s.%s < %s.%s", copyTableName, copyStagingTableName, copyTableName, mergePk, copyStagingTableName, mergePk, copyTableName, mergeTimestamp, copyStagingTableName, mergeTimestamp));
                    sqls.push(util.format("DELETE from %s using %s where %s.%s=%s.%s and %s.%s >= %s.%s", copyStagingTableName, copyTableName, copyStagingTableName, mergePk, copyTableName, mergePk, copyTableName, mergeTimestamp, copyStagingTableName, mergeTimestamp));
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

                collectedResponse.push({
                    "table_name": copyTableName,
                    "statement_name": statementName,
                    "statement_id": execResp.Id,
                    "manifest_file": manifestKey
                });
            }

            console.log(collectedResponse);

            return {
                "data_api_responses": collectedResponse
            };
        }
    }

    return {};
};
