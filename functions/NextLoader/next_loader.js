const AWS = require("aws-sdk");
const util = require("util");
const {loadSQLBuilder, loadTracker} = require("/opt/nodejs/load_sql_builder");

const rsData = new AWS.RedshiftData();
const RS_DATA_USER = "redshift_data_api_user";
exports.handler = async (event) => {
    const detail = event.detail;
    const statementName = detail.statementName;
    const tableName = statementName.substring(statementName.indexOf("/") + 1, statementName.lastIndexOf("_"));
    const dbName = statementName.substr(0, statementName.indexOf("/"));

    console.log("DB Name: "+dbName);
    console.log("Table Name: "+tableName);
    console.log("Statement Name: "+statementName);

    try {
        const nextItem = await loadTracker.trackNextExecution(dbName, tableName);

        if (nextItem != null) {
            const config = await loadSQLBuilder.getConfig(nextItem.event_source_arn);

            const copyOptions = config.copy.options;
            const copyRoleArn = config.copy.role_arn;
            const copyTableName = config.copy.table_name;
            const clusterIdentifier = config.cluster.identifier;
            const clusterDb = config.cluster.db_name;
            const mergeDuplicatePks = config.options.merge_duplicate_pks;
            const mergePk = config.options.merge_pk;
            const mergeTimestamp = config.options.merge_timestamp;
            const copyStagingTableName = copyTableName+"_"+Date.now();

            const manifestFiles = [{bucket: nextItem.manifest_bucket, key: nextItem.manifest_key}];
            const pendingRecords = await loadTracker.trackAllPendingBatch(dbName, tableName);
            if (pendingRecords && pendingRecords.length > 0) {
                for (const pendingRecord of pendingRecords) {
                    manifestFiles.push({
                        bucket: pendingRecord.manifest_bucket.S,
                        key: pendingRecord.manifest_key.S
                    });
                }
            }

            const execResp = await rsData.batchExecuteStatement({
                ClusterIdentifier: clusterIdentifier,
                Database: clusterDb,
                Sqls: loadSQLBuilder.generate(copyTableName, copyStagingTableName, mergeDuplicatePks, mergePk, mergeTimestamp, manifestFiles, copyRoleArn, copyOptions),
                DbUser: RS_DATA_USER,
                StatementName: statementName,
                WithEvent: true
            }).promise();
        
            const response = {
                "statement_name": nextItem.statement_name,
                "statement_id": execResp.Id
            }
    
            console.log(response);
    
            return response;
        }
    } catch (e) {
        console.log("TransactError: "+e);
    }

    return {};
}