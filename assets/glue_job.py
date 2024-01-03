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

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

event_df = glueContext.create_dynamic_frame.from_catalog(database = "cdk-database", table_name = "csv").toDF()
retail_uk = event_df.filter((col('Country') == 'United Kingdom') & (col('CustomerID').isNotNull()))
retail_uk = retail_uk.withColumn('TotalPrice', col('UnitPrice') * col('Quantity')).filter(col('TotalPrice') > 0)
total_retail_uk = (retail_uk.groupBy('InvoiceNo', 'CustomerID').agg(
    F.first('InvoiceDate').alias('InvoiceDate'),
    F.sum('TotalPrice').alias('TotalBill')).dropDuplicates(['InvoiceNo', 'CustomerID']))
    
rfm_df = total_retail_uk.groupBy('customerID').agg(
    F.sum('TotalBill').alias('Monetary'),
    F.count('invoiceNo').alias('Frequency')
)

rfm_dynamic_frame = DynamicFrame.fromDF(rfm_df, glueContext, "rfm_dynamic_frame")

datasink = glueContext.write_dynamic_frame.from_options(
    frame=rfm_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://processed-cdk-bucket-598915801708/"},
    format="parquet",
    transformation_ctx="datasink"
)

