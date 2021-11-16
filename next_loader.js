const AWS = require("aws-sdk");
const util = require("util");

const DDB_TRACKER = process.env.DDB_TRACKER;
const s3 = new AWS.S3();
const ddb = new AWS.DynamoDB();
const rsData = new AWS.RedshiftData();

const trackNextExecution = async(dbName, tableName) => {
    const params = {
        "TableName": DDB_TRACKER,
        "KeyConditionExpression": "db_table = :dbtable AND begins_with(status_timestamp, :status)",
        "ExpressionAttributeValues": {
            ":dbtable": {
                "S": dbName+"#"+tableName
            },
            ":status": {
                "S": "PENDING#"
            }
        },
        "Limit": 1,
        "ConsistentRead": true
    }

    const response = await ddb.query(params).promise();
    const items = response.Items;

    await ddb.deleteItem({
        "Key": {
            "db_table": {
                "S": dbName+"#"+tableName
            },
            "status_timestamp": {
                "S": "RUNNING"
            }
        },
        "TableName": DDB_TRACKER
    }).promise();

    if (items.length > 0) {
        const nextItem = {
            "db_table": items[0].db_table.S,
            "status_timestamp": items[0].status_timestamp.S,
            "payload_bucket": items[0].payload_bucket.S,
            "payload_key": items[0].payload_key.S,
        };

        nextItem.statement_name = nextItem.status_timestamp.substr(nextItem.status_timestamp.indexOf("#") + 1);

        const transactWriteParams = {
            "TransactItems": [
                {
                    "Delete": {
                        "Key": {
                            "db_table": {
                                "S": nextItem.db_table
                            },
                            "status_timestamp": {
                                "S": nextItem.status_timestamp
                            }
                        },
                        "TableName": DDB_TRACKER
                    }
                },
                {
                    "Put": {
                        "Item": {
                            "db_table": {
                                "S": nextItem.db_table
                            },
                            "status_timestamp": {
                                "S": "RUNNING"
                            },
                            "statement_name": {
                                "S": nextItem.statement_name
                            },
                            "payload_bucket": {
                                "S": nextItem.payload_bucket
                            },
                            "payload_key": {
                                "S": nextItem.payload_key
                            }
                        },
                        "TableName": DDB_TRACKER
                    }
                }
            ]
        };

        await ddb.transactWriteItems(transactWriteParams).promise();

        return nextItem;
    }

    return null;
}

exports.handler = async (event) => {
    const detail = event.detail;
    const statementName = detail.statementName;
    const tableName = statementName.substring(statementName.indexOf("/") + 1, statementName.lastIndexOf("_"));
    const dbName = statementName.substr(0, statementName.indexOf("/"));

    console.log("DB Name: "+dbName);
    console.log("Table Name: "+tableName);
    console.log("Statement Name: "+statementName);

    const nextItem = await trackNextExecution(dbName, tableName);

    if (nextItem != null) {
        const pendingPayloadResp = await s3.getObject({Bucket: nextItem.payload_bucket, Key: nextItem.payload_key}).promise();
        const pendingPayload = JSON.parse(pendingPayloadResp.Body.toString());
    
        const execResp = await rsData.batchExecuteStatement(pendingPayload).promise();
    
        const response = {
            "statement_name": nextItem.statement_name,
            "statement_id": execResp.Id
        }

        console.log(response);

        return response;
    }

    return {};
}