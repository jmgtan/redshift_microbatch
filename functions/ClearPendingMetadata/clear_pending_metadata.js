const AWS = require("aws-sdk");
const util = require("util");
const chunk = require("chunk");

const DDB_TRACKER = process.env.DDB_TRACKER;
const ddb = new AWS.DynamoDB();
const BATCH_SIZE = 20;

exports.handler = async(event) => {
    const dbTable = event.db_table;

    let lastEvaluatedKey = null;
    const recordsToDelete = [];

    do {
        const params = {
            "TableName": DDB_TRACKER,
            "KeyConditionExpression": "db_table = :dbtable AND begins_with(status_timestamp, :status)",
            "ProjectionExpression": "db_table, status_timestamp",
            "ExpressionAttributeValues": {
                ":dbtable": {
                    "S": dbTable
                },
                ":status": {
                    "S": "PENDING#"
                }
            },
            "ExclusiveStartKey": lastEvaluatedKey
        }

        const response = await ddb.query(params).promise();

        const items = response.Items;

        if (items && items.length > 0) {
            for (var i in items) {
                const item = items[i];
                recordsToDelete.push(item);
            }
        }

        lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey != null);

    if (recordsToDelete.length > 0) {
        const batched = chunk(recordsToDelete, BATCH_SIZE);

        for (var i in batched) {
            const batch = batched[i];
            const batchRequestParam = {"RequestItems": {}};
            batchRequestParam["RequestItems"][DDB_TRACKER] = [];

            for (var j in batch) {
                const batchItem = batch[j];
                batchRequestParam["RequestItems"][DDB_TRACKER].push({
                    "DeleteRequest": {
                        "Key": batchItem
                    }
                });
            }

            await ddb.batchWriteItem(batchRequestParam).promise();
        }
    }
}