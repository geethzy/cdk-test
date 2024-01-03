from constructs import Construct
import aws_cdk as cdk
import aws_cdk.aws_s3 as _s3
import aws_cdk.aws_iam as _iam
import aws_cdk.aws_sqs as _sqs
import aws_cdk.aws_glue as _glue
from constructs import Construct
import aws_cdk.aws_events as _events
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_lakeformation as _lakeformation

class CdktestStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        
        raw_bucket = _s3.Bucket(self, 'raw_data', 
                                bucket_name=f'raw-cdk-bucket-{cdk.Aws.ACCOUNT_ID}',
                                removal_policy=cdk.RemovalPolicy.DESTROY, 
                                event_bridge_enabled=True,
                                lifecycle_rules=[_s3.LifecycleRule( 
                                    transitions=[_s3.Transition(
                                        storage_class=_s3.StorageClass.GLACIER, 
                                        transition_after=cdk.Duration.days(7))])]) 


        output_bucket = _s3.Bucket(self, 'processed_cdk_bucket',
                   bucket_name=f'processed-cdk-bucket-{cdk.Aws.ACCOUNT_ID}',
                   auto_delete_objects=True,
                   removal_policy=cdk.RemovalPolicy.DESTROY)

        s3deploy.BucketDeployment(self, 'deployment_scripts',
                          sources=[s3deploy.Source.asset('assets/')],
                          destination_bucket=output_bucket,
                          destination_key_prefix='scripts/')

        glue_queue = _sqs.Queue(self, 'glue_queue')
        raw_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED, s3n.SqsDestination(glue_queue))

        glue_role = _iam.Role(self, 'glue_role',
                              role_name='cdkGlueRole',
                              description='Role for Glue services to access S3',
                              assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'), 
                              inline_policies={'glue_policy': _iam.PolicyDocument(statements=[_iam.PolicyStatement(
                                  effect=_iam.Effect.ALLOW,
                                  actions=['s3:*', 'glue:*', 'iam:*', 'logs:*', 'cloudwatch:*', 'sqs:*',
                                           'cloudtrail:*'],
                                  resources=['*'])])})

        glue_database = _glue.CfnDatabase(self, 'glue-database',
                                          catalog_id=cdk.Aws.ACCOUNT_ID,
                                          database_input=_glue.CfnDatabase.DatabaseInputProperty(
                                              name='cdk-database',
                                              description='Database to store csv data.'))

        # _lakeformation.CfnPermissions(self, 'lakeformation_permission',
        #                               data_lake_principal=_lakeformation.CfnPermissions.DataLakePrincipalProperty(
        #                                   data_lake_principal_identifier=glue_role.role_arn),
        #                               resource=_lakeformation.CfnPermissions.ResourceProperty(
        #                                   database_resource=_lakeformation.CfnPermissions.DatabaseResourceProperty(
        #                                       catalog_id=cdk.Aws.ACCOUNT_ID,
        #                                       name='cdk-database')),
        #                               permissions=['ALL'])

       
        rule_role = _iam.Role(self, 'rule_role',
                              role_name='EventBridgeRole',
                              description='Role for EventBridge to trigger Glue workflows.',
                              assumed_by=_iam.ServicePrincipal('events.amazonaws.com'),
                              inline_policies={
                                  'eventbridge_policy': _iam.PolicyDocument(statements=[_iam.PolicyStatement(
                                      effect=_iam.Effect.ALLOW,
                                      actions=['events:*', 'glue:*'],
                                      resources=['*'])])})

        _events.CfnRule(self, 'rule_s3_glue',
                        name='rule_s3_glue',
                        role_arn=rule_role.role_arn,
                        targets=[_events.CfnRule.TargetProperty(
                            arn=f'arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow',
                            role_arn=rule_role.role_arn,
                            id=cdk.Aws.ACCOUNT_ID)],
                        event_pattern={
                            "detail-type": ["Object Created"],
                            "detail": {
                                "bucket": {"name": [f"{raw_bucket.bucket_name}"]}},
                            "source": ["aws.s3"]})
        
        _glue.CfnCrawler(self, 'glue_crawler',
                         name='glue_crawler',
                         role=glue_role.role_arn,
                         database_name='cdk-database',
                         targets=_glue.CfnCrawler.TargetsProperty(
                             s3_targets=[_glue.CfnCrawler.S3TargetProperty(
                                 path=f's3://{raw_bucket.bucket_name}/csv/',
                                 event_queue_arn=glue_queue.queue_arn)]),
                         recrawl_policy=_glue.CfnCrawler.RecrawlPolicyProperty(
                             recrawl_behavior='CRAWL_EVENT_MODE'))

        # glue_job = _glue.CfnJob(self, 'glue_job',
        #                         name='glue_job',
        #                         command=_glue.CfnJob.JobCommandProperty(
        #                             name='pythonshell',
        #                             python_version='3.9',
        #                             script_location=f's3://{output_bucket.bucket_name}/scripts/glue_job.py'),
        #                         role=glue_role.role_arn,
        #                         glue_version='3.0',
        #                         timeout=3
        #                         # worker_type='G.1X',
        #                         # number_of_workers=5
        #                         )

        glue_job = _glue.CfnJob(self, 'glue_job',
                        name='glue_job',
                        command=_glue.CfnJob.JobCommandProperty(
                            name='glueetl',
                            python_version='3',
                            script_location=f's3://{output_bucket.bucket_name}/scripts/glue_job.py'),
                        role=glue_role.role_arn,
                        glue_version='3.0',
                        worker_type='Standard',
                        number_of_workers=5,
                        max_retries=0, 
                        timeout=180,  
                        execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                            max_concurrent_runs=1)  
                        )


        glue_workflow = _glue.CfnWorkflow(self, 'glue_workflow',
                                          name='glue_workflow',
                                          description='Workflow to process the rfm data.')

        _glue.CfnTrigger(self, 'glue_crawler_trigger',
                         name='glue_crawler_trigger',
                         actions=[_glue.CfnTrigger.ActionProperty(
                             crawler_name='glue_crawler',
                             notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                             timeout=3)],
                         type='EVENT',
                         workflow_name=glue_workflow.name)
        
        _glue.CfnTrigger(self, 'glue_job_trigger',
                         name='glue_job_trigger',
                         actions=[_glue.CfnTrigger.ActionProperty(
                             job_name=glue_job.name,
                             notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                             timeout=3)],
                         type='CONDITIONAL',
                         start_on_creation=True,
                         workflow_name=glue_workflow.name,
                         predicate=_glue.CfnTrigger.PredicateProperty(
                             conditions=[_glue.CfnTrigger.ConditionProperty(
                                 crawler_name='glue_crawler',
                                 logical_operator='EQUALS',
                                 crawl_state='SUCCEEDED')]))



        


