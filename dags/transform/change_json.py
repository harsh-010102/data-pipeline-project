import json
import pandas as pd
import boto3
from auth import ACCESS_KEY, SECRET_KEY

client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, verify=False)
session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
s3 = session.resource('s3', verify=False)
Bucket = 'destination-bucket5500'
source_bucket_name = 'source-bucket5500'

'''
    File to transform the json data as per needed
'''
def filterData(filename):
    response = client.get_object(Bucket=source_bucket_name, Key=filename)
    data = json.loads(response['Body'].read())

    # load json formatted data to dataframe
    df = pd.json_normalize(data, 'items')
    df.rename(columns={'snippet.assignable': 'assignable', 'snippet.channelId': 'channelId', 'snippet.title': 'title'},
              inplace=True)
    # removing unnecessary columns from the dataset
    df = df.drop(['kind'], axis=1)
    df = df.drop(['assignable'], axis=1)
    csv_buffer = df.to_csv(index=False)
    client.put_object(Body=csv_buffer.encode(), Bucket=Bucket, Key=filename)
