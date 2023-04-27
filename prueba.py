import boto3
import pandas as pd

s3 = boto3.client(
    's3',
    aws_access_key_id='AKIA3Y5GLSN2SMV5K57S',
    aws_secret_access_key='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'#,
    #region_name='us-east-1'
)

bucket_name = 'prueba-udesa'
file_name = 'advertiser_ids.csv'
local_file_name = 'archivo.csv'

s3.download_file(bucket_name, file_name, local_file_name)

data = pd.read_csv(local_file_name)
print(data)