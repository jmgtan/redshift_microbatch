const AWS = require("aws-sdk");
const util = require("util");
const VALID_ERROR_TO_RETRY = "Serializable isolation violation";
const WAIT_TIME = 30000;
const RS_DATA_USER = "redshift_data_api_user";
const COPY_IAM_ROLE_ARN = process.env.COPY_IAM_ROLE_ARN;

const sleep = async (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    })
}

exports.handler = async (event) => {
    const rsData = new AWS.RedshiftData();
    const detail = event.detail;
    
    if (detail.state == "FAILED") {
        const statementName = detail.statementName;
        const statementId = detail.statementId;
        const clusterDb = statementName.substr(0, statementName.indexOf("/"));

        const statementDetails = await rsData.describeStatement({Id: statementId}).promise();

        if (statementDetails.Error.indexOf(VALID_ERROR_TO_RETRY) != -1) {
            const sqls = [];
            const statementName = clusterDb+"/retry/"+statementId;

            for (i in statementDetails.SubStatements) {
                const subStatement = statementDetails.SubStatements[i];
                var queryString = subStatement.QueryString;

                if (queryString.startsWith("COPY")) {
                    queryString = queryString.replace("iam_role ''", util.format("iam_role '%s'", COPY_IAM_ROLE_ARN));
                }

                sqls.push(queryString);
            }

            sleep(WAIT_TIME);

            const execResp = await rsData.batchExecuteStatement({
                ClusterIdentifier: statementDetails.ClusterIdentifier,
                Database: clusterDb,
                Sqls: sqls,
                DbUser: RS_DATA_USER,
                StatementName: statementName,
                WithEvent: true
            }).promise();

            const response = {
                "statement_name": statementName,
                "statement_id": execResp.Id,
                "retry_statement_id": statementId
            }

            console.log(response);

            return response;
        }
    }

    return {};
}