const AWS = require("aws-sdk");
const util = require("util");

const RS_DATA_USER = "redshift_data_api_user";

exports.handler = async (event) => {
    const pathsToLoad = event.paths;
    const copyRoleArn = event.copy.role_arn;
    const clusterIdentifier = event.cluster.identifier;
    const clusterDb = event.cluster.db_name;
    var responses = [];

    if (pathsToLoad.length > 0) {
        const rsData = new AWS.RedshiftData();

        for (var i=0;i<pathsToLoad.length;i++) {
            const path = pathsToLoad[i];
            const s3Path = path.s3_path;
            const copyTableName = path.table;
            const mergeDuplicatePks = path.options.merge_duplicate_pks;
            const mergePk = path.options.merge_pk;
            const mergeTimestamp = path.options.merge_timestamp;
            const copyStagingTableName = copyTableName+"_"+Date.now();

            var sqls = [
                util.format("CREATE TEMP TABLE %s as select * from %s where 1=2", copyStagingTableName, copyTableName),
                util.format("COPY %s from '%s' iam_role '%s' format as parquet", copyStagingTableName, s3Path, copyRoleArn),
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
                WithEvent: true
            }).promise();

            responses.push({
                "s3_path": s3Path,
                "table": copyTableName,
                "statement_id": execResp.Id
            });
        }

        return responses;
    }

    return {};
}