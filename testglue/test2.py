import sys
import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, to_date, datediff, current_date, max as spark_max, sum, first

# @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# # job.init(args['testrfmjob'], args)
# job.init('glue_job')

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
event_id = glueContext.create_dynamic_frame.from_options(connection_type = "s3", connection_options = {"paths": [f"s3://YOUR_BUCKET/PREFIX/"]}).toDF().filter(col('name') == args['WORKFLOW_NAME']).select(col('id')).collect()[0][0]

event_df = glueContext.create_dynamic_frame.from_catalog(database = "cdk-database", table_name = "csv").toDF()
filtered_events = event_df.filter((col('EventName') == 'NotifyEvent') & (col('timestamp') >= (datetime.datetime.now() - datetime.timedelta(minutes=5))))
events = filtered_events.collect()

for event in events:
    event_payload = json.loads(event['CloudTrailEvent'])['requestParameters']['eventPayload']
    if event_payload['eventId'] == event_id:
        object_key = json.loads(event_payload['eventBody'])['detail']['object']['key']
        bucket_name = json.loads(event_payload['eventBody'])['detail']['bucket']['name']

# dyf = glueContext.create_dynamic_frame.from_catalog(database='cdk_database', table_name='csv')
# retail_uk = dyf.toDF()
retail_uk = spark.read.format("s3").load(f"s3://raw-cdk-bucket-598915801708/csv")
retail_uk = retail_uk.filter((col('Country') == 'United Kingdom') & (col('CustomerID').isNotNull()))
retail_uk = retail_uk.withColumn('TotalPrice', col('UnitPrice') * col('Quantity')).filter(col('TotalPrice') > 0)
total_retail_uk = (retail_uk.groupBy('InvoiceNo', 'CustomerID').agg(
    F.first('InvoiceDate').alias('InvoiceDate'),
    F.sum('TotalPrice').alias('TotalBill')).dropDuplicates(['InvoiceNo', 'CustomerID']))
    
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
total_retail_uk = total_retail_uk.withColumn('Date', to_date(total_retail_uk['Invoicedate'], 'MM/dd/yyyy HH:mm').cast('date'))
total_retail_uk = total_retail_uk.withColumn('CurrentDate', current_date())
total_retail_uk = total_retail_uk.withColumn('DaysDifference', datediff(total_retail_uk['CurrentDate'], total_retail_uk['Date']))    

rfm_df = total_retail_uk.groupBy('customerID').agg(
    F.sum('TotalBill').alias('Monetary'),
    F.count('invoiceNo').alias('Frequency')
    F.min('DaysDifference').alias('Recency')
)

rfm_dynamic_frame = DynamicFrame.fromDF(rfm_df, glueContext, "rfm_dynamic_frame")

datasink = glueContext.write_dynamic_frame.from_options(
    frame=rfm_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://processed-cdk-bucket-598915801708/"},
    format="parquet",
    transformation_ctx="datasink"
)

# job.commit()
