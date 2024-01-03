import boto3

aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
region_name = 'YOUR_REGION'
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
bucket_name = 'YOUR_BUCKET_NAME'
file_path = '/path/to/your/file.txt'
s3_key = 'file.txt'
s3.upload_file(file_path, bucket_name, s3_key)
