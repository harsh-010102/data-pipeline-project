import pandas as pd
import boto3
from auth import ACCESS_KEY, SECRET_KEY

session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
s3 = session.resource('s3', verify=False)
client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, verify=False)
Bucket = 'destination-bucket5500'
my_bucket = 'source-bucket5500'

'''
File to transform the csv data as per needed
'''
def filterData(filename):
    key = filename
    csv_obj = client.get_object(Bucket=my_bucket, Key=key)
    df = pd.read_csv(csv_obj['Body'])

    # find the country details and append country code as per country values
    df['Country_Code'] = ''
    if 'usa_videos' in key:
        df['Country_Code'] = 3
    elif 'canada_videos' in key:
        df['Country_Code'] = 2
    else:
        df['Country_Code'] = 1

    # Drop some unncessary columns so that to reduce complexity of the data
    df = df.drop(['trending_date', 'tags', 'publish_time', 'thumbnail_link', 'video_error_or_removed',
                  'comments_disabled'], axis=1)
    df = df.drop(['ratings_disabled'], axis=1)
    df = df.drop(['description'], axis=1)

    #  convert dataframe to csv file and store it temporarily in csv bufferW
    csv_buffer = df.to_csv(index=False)
    client.put_object(Body=csv_buffer.encode(), Bucket='destination-bucket5500', Key=filename)
