import * as cdk from '@aws-cdk/core';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import * as eb from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';
import { Effect, ManagedPolicy } from '@aws-cdk/aws-iam';
import { NodejsFunction } from '@aws-cdk/aws-lambda-nodejs';
import { Duration } from '@aws-cdk/core';
import { Code, Function, Runtime } from '@aws-cdk/aws-lambda';

export class RedshiftMicrobatchStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const paramStatementNamePrefix = new cdk.CfnParameter(this, 'statementNamePrefix', {
      type: "String",
      description: "Statement name prefix filter for EventBridge rule. Set this to the database name. Default value: 'dev'",
      default: "dev"
    });

    const trackingTable = new dynamodb.Table(this, 'RedshiftLoadTracker', {
      partitionKey: { name: 'db_table', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'status_timestamp', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
    });

    const bucket = new s3.Bucket(this, 'RedshiftLoadBucket');

    const loaderRolePolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes"
          ],
          resources: ["*"]
        }),
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "s3:GetObject",
            "s3:PutObject"
          ],
          resources: [bucket.bucketArn+"/*"]
        }),
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "dynamodb:*"
          ],
          resources: [trackingTable.tableArn]
        })
      ]
    });

    const loaderRole = new iam.Role(this, "LoaderRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
        ManagedPolicy.fromAwsManagedPolicyName("AmazonRedshiftDataFullAccess")
      ],
      inlinePolicies: {inline0: loaderRolePolicy}
    });

    const microbatchLoaderFunction = new Function(this, "MicrobatchLoaderFunction", {
      code: Code.fromAsset(__dirname + "/../../functions/MicrobatchLoader/"),
      runtime: Runtime.NODEJS_14_X,
      handler: "loader.handler",
      timeout: Duration.minutes(2),
      environment: {
        "CONFIG_BUCKET": bucket.bucketName,
        "CONFIG_PREFIX": "rs-loader-config/",
        "DDB_TRACKER": trackingTable.tableName,
        "PENDING_BUCKET": bucket.bucketName,
        "PENDING_PREFIX": "pending/",
        "MANIFEST_BUCKET": bucket.bucketName,
        "MANIFEST_PREFIX": "manifest_files/"
      },
      role: loaderRole
    });

    const nextLoaderFunction = new Function(this, "NextLoaderFunction", {
      code: Code.fromAsset(__dirname + "/../../functions/NextLoader/"),
      runtime: Runtime.NODEJS_14_X,
      handler: "next_loader.handler",
      timeout: Duration.minutes(2),
      environment: {
        "DDB_TRACKER": trackingTable.tableName
      },
      role: loaderRole
    });

    const nextLoaderEvent = new eb.Rule(this, "NextLoaderRule", {
      eventPattern: {
        source: ["aws.redshift-data"],
        detailType: ["Redshift Data Statement Status Change"],
        detail: {
          state: ["FINISHED"],
          statementName: [{
            "prefix": paramStatementNamePrefix.valueAsString + "/"
          }]
        }
      },
      targets: [
        new targets.LambdaFunction(nextLoaderFunction)
      ]
    });

    const bulkLoaderFunction = new Function(this, "BulkLoaderFunction", {
      code: Code.fromAsset(__dirname + "/../../functions/BulkLoader/"),
      runtime: Runtime.NODEJS_14_X,
      handler: "bulk_loader.handler",
      timeout: Duration.minutes(5),
      role: loaderRole
    });

    const clearPendingMetadataFunction = new Function(this, "ClearPendingMetadataFunction", {
      code: Code.fromAsset(__dirname + "/../../functions/ClearPendingMetadata/"),
      runtime: Runtime.NODEJS_14_X,
      handler: "clear_pending_metadata.handler",
      timeout: Duration.minutes(5),
      role: loaderRole
    });
  }
}
