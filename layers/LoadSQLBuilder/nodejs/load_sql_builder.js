const AWS = require("aws-sdk");
const util = require("util");
const chunk = require("chunk");
const BATCH_SIZE = 10;

const DDB_TRACKER = process.env.DDB_TRACKER;
const ddb = new AWS.DynamoDB();
const s3 = new AWS.S3();

const configBucket = process.env.CONFIG_BUCKET;
var configPrefix = process.env.CONFIG_PREFIX;

if (!configPrefix.endsWith("/")) {
    configPrefix += "/";
}

const configCache = {};

const loadSQLBuilder = {
    getConfig: async(eventSourceARN) => {
        const queue = eventSourceARN.substr(eventSourceARN.lastIndexOf(":") + 1);

        if (!(queue in configCache)) {
            const configKey = configPrefix + queue + ".json";
            const configResponse = await s3.getObject({Bucket: configBucket, Key: configKey}).promise();
            const config = JSON.parse(configResponse.Body.toString());
    
            configCache[queue] = config;
        }
    
        return configCache[queue];
    },
    generate: (copyTableName, copyStagingTableName, mergeDuplicatePks, mergePk, mergeTimestamp, manifestFiles, copyRoleArn, copyOptions) => {
        const sqls = [
            util.format("CREATE TEMP TABLE %s (like %s)", copyStagingTableName, copyTableName)
        ];
        
        for (const manifestFile of manifestFiles) {
            sqls.push(util.format("COPY %s from 's3://%s/%s' iam_role '%s' format as parquet manifest %s", copyStagingTableName, manifestFile.bucket, manifestFile.key, copyRoleArn, copyOptions));
        }

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

        return sqls;
    }
}

const loadTracker = {
    track: async(dbName, tableName, statementName) => {
        await ddb.putItem({
            "TableName": DDB_TRACKER,
            "Item": {
                "db_table": {
                    "S": dbName + "#" + tableName
                },
                "status_timestamp": {
                    "S": "RUNNING"
                },
                "statement_name": {
                    "S": statementName
                }
            },
            "ConditionExpression": "attribute_not_exists(db_table) and attribute_not_exists(status_timestamp)"
        }).promise();
    },
    queue: async(dbName, tableName, statementName, manifestBucket, manifestKey, eventSourceARN) => {
        const params = {
            "TableName": DDB_TRACKER,
            "Item": {
                "db_table": {
                    "S": dbName + "#" + tableName
                },
                "status_timestamp": {
                    "S": "PENDING#" + statementName
                },
                "statement_name": {
                    "S": statementName
                },
                "manifest_bucket": {
                    "S": manifestBucket
                },
                "manifest_key": {
                    "S": manifestKey
                },
                "event_source_arn": {
                    "S": eventSourceARN
                }
            }
        }
    
        await ddb.putItem(params).promise();
    },
    trackAllPendingBatch: async(dbName, tableName) => {
        let recordsToLoad = [];
        const dbTable = dbName+"#"+tableName;
        
        const params = {
            "TableName": DDB_TRACKER,
            "KeyConditionExpression": "db_table = :dbtable AND begins_with(status_timestamp, :status)",
            "ExpressionAttributeValues": {
                ":dbtable": {
                    "S": dbTable
                },
                ":status": {
                    "S": "PENDING#"
                }
            }
        }

        const response = await ddb.query(params).promise();
        console.log("Track Pending");
        console.log(response);

        const items = response.Items;

        if (items && items.length > 0) {
            recordsToLoad = recordsToLoad.concat(items);
        }

        console.log("Updating with: "+recordsToLoad.length);

        await ddb.updateItem({
            "TableName": DDB_TRACKER,
            "Key": {
                "db_table": {
                    "S": dbTable
                },
                "status_timestamp": {
                    "S": "RUNNING"
                }
            },
            "UpdateExpression": "set batch_count = :batchCount",
            "ExpressionAttributeValues": {
                ":batchCount": {
                    "N": ""+recordsToLoad.length
                }
            }
        }).promise();

        return recordsToLoad;
    },
    trackNextExecution: async(dbName, tableName) => {
        const dbTable = dbName+"#"+tableName;
        
        const currentRunning = await ddb.getItem({
            "TableName": DDB_TRACKER,
            "Key": {
                "db_table": {
                    "S": dbTable
                },
                "status_timestamp": {
                    "S": "RUNNING"
                }
            }
        }).promise();
        const currentRunningItem = currentRunning.Item;
        if (currentRunningItem != null && "batch_count" in currentRunningItem) {
            const batchCount = currentRunningItem.batch_count.N;

            if (batchCount > 0) {
                let remaining = batchCount;
                let recordsToDelete = [];
                let lastEvaluatedKey = null;
                
                do {
                    const records = await ddb.query({
                        "TableName": DDB_TRACKER,
                        "KeyConditionExpression": "db_table = :dbtable AND begins_with(status_timestamp, :status)",
                        "ExpressionAttributeValues": {
                            ":dbtable": {
                                "S": dbTable
                            },
                            ":status": {
                                "S": "PENDING#"
                            }
                        },
                        "ExclusiveStartKey": lastEvaluatedKey,
                        "Limit": remaining,
                        "ProjectionExpression": "db_table, status_timestamp"
                    }).promise();

                    recordsToDelete = recordsToDelete.concat(records.Items);
                    remaining -= records.Items.length;

                    if (remaining > 0) {
                        lastEvaluatedKey = records.LastEvaluatedKey;
                    }

                } while (remaining > 0);

                if (recordsToDelete.length > 0) {
                    recordsToDelete = chunk(recordsToDelete, BATCH_SIZE);

                    for (const chunkRecordsToDelete of recordsToDelete) {
                        const requestItems = [];
                        for (const chunkRecordToDelete of chunkRecordsToDelete) {
                            requestItems.push({
                                "DeleteRequest": {
                                    "Key": {
                                        "db_table": {
                                            "S": dbTable
                                        },
                                        "status_timestamp": {
                                            "S": chunkRecordToDelete.status_timestamp.S
                                        }
                                    }
                                }
                            })
                        }
                        const deleteParams = {"RequestItems": {}};
                        deleteParams["RequestItems"][DDB_TRACKER] = requestItems;
                        await ddb.batchWriteItem(deleteParams).promise();
                    }
                }
            }
        }

        const params = {
            "TableName": DDB_TRACKER,
            "KeyConditionExpression": "db_table = :dbtable AND begins_with(status_timestamp, :status)",
            "ExpressionAttributeValues": {
                ":dbtable": {
                    "S": dbTable
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
                    "S": dbTable
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
                "manifest_bucket": items[0].manifest_bucket.S,
                "manifest_key": items[0].manifest_key.S,
                "event_source_arn": items[0].event_source_arn.S
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
                                "manifest_bucket": {
                                    "S": nextItem.manifest_bucket
                                },
                                "manifest_key": {
                                    "S": nextItem.manifest_key
                                },
                                "event_source_arn": {
                                    "S": nextItem.event_source_arn
                                }
                            },
                            "TableName": DDB_TRACKER,
                            "ConditionExpression": "attribute_not_exists(db_table) and attribute_not_exists(status_timestamp)"
                        }
                    }
                ]
            };
    
            await ddb.transactWriteItems(transactWriteParams).promise();
    
            return nextItem;
        }
    
        return null;
    }
}

exports.loadSQLBuilder = loadSQLBuilder;
exports.loadTracker = loadTracker;