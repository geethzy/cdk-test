// import * as cdk from 'aws-cdk-lib';
// import * as s3 from 'aws-cdk-lib/aws-s3';
// import * as iam from 'aws-cdk-lib/aws-iam';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
// import * as glue from 'aws-cdk-lib/aws-glue';
// import * as events from 'aws-cdk-lib/aws-events';
// import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
// import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
// import * as lakeformation from 'aws-cdk-lib/aws-lakeformation';

// export class CdktestStack extends cdk.Stack {
//     constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
//         super(scope, id, props);

//         const rawBucket = new s3.Bucket(this, 'raw_data', {
//             bucketName: `raw-cdk-bucket-${cdk.Aws.ACCOUNT_ID}`,
//             removalPolicy: cdk.RemovalPolicy.DESTROY,
//             lifecycleRules: [
//                 {
//                     transitions: [
//                         {
//                             storageClass: s3.StorageClass.GLACIER,
//                             transitionAfter: cdk.Duration.days(7)
//                         }
//                     ]
//                 }
//             ]
//         });

//         const outputBucket = new s3.Bucket(this, 'processed_cdk_bucket', {
//             bucketName: `processed-cdk-bucket-${cdk.Aws.ACCOUNT_ID}`,
//             autoDeleteObjects: true,
//             removalPolicy: cdk.RemovalPolicy.DESTROY
//         });

//         new s3deploy.BucketDeployment(this, 'deployment_scripts', {
//             sources: [s3deploy.Source.asset('assets/')],
//             destinationBucket: outputBucket,
//             destinationKeyPrefix: 'scripts/'
//         });

//         const glueQueue = new sqs.Queue(this, 'glue_queue');

//         rawBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.SqsDestination(glueQueue));

//         const glueRole = new iam.Role(this, 'glue_role', {
//             roleName: 'cdkGlueRole',
//             assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
//             inlinePolicies: {
//                 'glue_policy': new iam.PolicyDocument({
//                     statements: [
//                         new iam.PolicyStatement({
//                             effect: iam.Effect.ALLOW,
//                             actions: [
//                                 's3:*',
//                                 'glue:*',
//                                 'iam:*',
//                                 'logs:*',
//                                 'cloudwatch:*',
//                                 'sqs:*',
//                                 'cloudtrail:*'
//                             ],
//                             resources: ['*']
//                         })
//                     ]
//                 })
//             }
//         });

//         const glueDatabase = new glue.CfnDatabase(this, 'glue-database', {
//             catalogId: cdk.Aws.ACCOUNT_ID,
//             databaseInput: {
//                 name: 'cdk-database',
//                 description: 'Database to store csv data.'
//             }
//         });

//         // LakeFormation permission code is commented out

//         const ruleRole = new iam.Role(this, 'rule_role', {
//             roleName: 'EventBridgeRole',
//             assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
//             inlinePolicies: {
//                 'eventbridge_policy': new iam.PolicyDocument({
//                     statements: [
//                         new iam.PolicyStatement({
//                             effect: iam.Effect.ALLOW,
//                             actions: [
//                                 'events:*',
//                                 'glue:*'
//                             ],
//                             resources: ['*']
//                         })
//                     ]
//                 })
//             }
//         });

//         new events.CfnRule(this, 'rule_s3_glue', {
//             name: 'rule_s3_glue',
//             roleArn: ruleRole.roleArn,
//             targets: [
//                 {
//                     arn: `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow`,
//                     roleArn: ruleRole.roleArn,
//                     id: cdk.Aws.ACCOUNT_ID
//                 }
//             ],
//             eventPattern: {
//                 'detail-type': ['Object Created'],
//                 'detail': {
//                     'bucket': { 'name': [`${rawBucket.bucketName}`] }
//                 },
//                 'source': ['aws.s3']
//             }
//         });

//         new glue.CfnCrawler(this, 'glue_crawler', {
//             name: 'glue_crawler',
//             role: glueRole.roleArn,
//             databaseName: 'cdk-database',
//             targets: {
//                 s3Targets: [
//                     {
//                         path: `s3://${rawBucket.bucketName}/csv/`,
//                         eventQueueArn: glueQueue.queueArn
//                     }
//                 ]
//             },
//             recrawlPolicy: {
//                 recrawlBehavior: 'CRAWL_EVENT_MODE'
//             }
//         });

//         const glueJob = new glue.CfnJob(this, 'glue_job', {
//             name: 'glue_job',
//             command: {
//                 name: 'glueetl',
//                 pythonVersion: '3',
//                 scriptLocation: `s3://${outputBucket.bucketName}/scripts/glue_job.py`
//             },
//             role: glueRole.roleArn,
//             glueVersion: '3.0',
//             workerType: 'Standard',
//             numberOfWorkers: 5,
//             maxRetries: 0,
//             timeout: 180,
//             executionProperty: {
//                 maxConcurrentRuns: 1
//             }
//         });

//         const glueWorkflow = new glue.CfnWorkflow(this, 'glue_workflow', {
//             name: 'glue_workflow',
//             description: 'Workflow to process the rfm data.'
//         });

//         new glue.CfnTrigger(this, 'glue_crawler_trigger', {
//             name: 'glue_crawler_trigger',
//             actions: [
//                 {
//                     crawlerName: 'glue_crawler',
//                     notificationProperty: { notifyDelayAfter: 3 },
//                     timeout: 3
//                 }
//             ],
//             type: 'EVENT',
//             workflowName: glueWorkflow.name
//         });

//         new glue.CfnTrigger(this, 'glue_job_trigger', {
//             name: 'glue_job_trigger',
//             actions: [
//                 {
//                     jobName: glueJob.name,
//                     notificationProperty: { notifyDelayAfter: 3 },
//                     timeout: 3
//                 }
//             ],
//             type: 'CONDITIONAL',
//             startOnCreation: true,
//             workflowName: glueWorkflow.name,
//             predicate: {
//                 conditions: [
//                     {
//                         crawlerName: 'glue_crawler',
//                         logicalOperator: 'EQUALS',
//                         crawlState: 'SUCCEEDED'
//                     }
//                 ]
//             }
//         });
//     }
// }
