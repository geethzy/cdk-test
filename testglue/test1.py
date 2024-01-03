import io
import os
import sys
import json
import boto3
import urllib3
import datetime
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions


class Utils:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue')
        self.event_client = boto3.client('cloudtrail')

    def get_data_from_s3(self):

        self.args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        self.event_id = self.glue_client.get_workflow_run_properties(Name=self.args['WORKFLOW_NAME'],
                                                                     RunId=self.args['WORKFLOW_RUN_ID'])[
                            'RunProperties'][
                            'aws:eventIds'][1:-1]

        response = self.event_client.lookup_events(LookupAttributes=[{'AttributeKey': 'EventName',
                                                                      'AttributeValue': 'NotifyEvent'}],
                                                   StartTime=(datetime.datetime.now() - datetime.timedelta(minutes=5)),
                                                   EndTime=datetime.datetime.now())['Events']
        
        for i in range(len(response)):
            event_payload = json.loads(response[i]['CloudTrailEvent'])['requestParameters']['eventPayload']
            if event_payload['eventId'] == self.event_id:
                self.object_key = json.loads(event_payload['eventBody'])['detail']['object']['key']
                self.bucket_name = json.loads(event_payload['eventBody'])['detail']['bucket']['name']
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.object_key)

        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    
utils = Utils()
rfm_data = utils.get_data_from_s3()

retail_uk = rfm_data[(rfm_data['Country'] == 'United Kingdom') & (rfm_data['CustomerID'].notnull())]
retail_uk['TotalPrice'] = retail_uk['UnitPrice'] * retail_uk['Quantity']
retail_uk = retail_uk[retail_uk['TotalPrice'] > 0]
total_retail_uk = retail_uk.groupby(['InvoiceNo', 'CustomerID']).agg({
    'InvoiceDate': 'first',
    'TotalPrice': 'sum'
}).reset_index()

total_retail_uk['Date'] = pd.to_datetime(total_retail_uk['InvoiceDate'], format='%m/%d/%Y %H:%M').dt.date
total_retail_uk['CurrentDate'] = datetime.now().date()
total_retail_uk['DaysDifference'] = (total_retail_uk['CurrentDate'] - total_retail_uk['Date']).dt.days

rfm_df = total_retail_uk.groupby('CustomerID').agg({
    'TotalPrice': 'sum',
    'InvoiceNo': 'count',
    'DaysDifference': 'min'
}).rename(columns={
    'TotalPrice': 'Monetary',
    'InvoiceNo': 'Frequency',
    'DaysDifference': 'Recency'
}).reset_index()

dataframes = {'rfm_df': rfm_df}
df_name='rfm_df'
wr.s3.to_csv(dataframes[df_name], path=f's3://output_bucket/processed/{df_name}.csv', index=False)

# s3 = boto3.resource('s3')
# rfm_df.to_csv(io.StringIO(), index=False)
# s3.Object(bucket_name, file_key).put(Body=io.StringIO().getvalue())