import configparser
import csv
from datetime import datetime, timedelta
import time
import redshift_connector
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from auth import ACCESS_KEY, SECRET_KEY
from transform import change_csv, change_json
import boto3
from botocore.exceptions import ClientError


'''
    Created By Harsh Shah
    At Encora Digital Pvt. Ltd.
    Project Title - Complete ETL pipeline of Youtube trending dataset
    Client - Personal Project
'''

# To get content from the config file . Data warehouse information is stored in this config file

config = configparser.ConfigParser()
config.read_file(open('/opt/finalDEproject/data/cluster.config'))

# List all the connection that is being required in the entire project

ec2 = boto3.client('ec2', aws_access_key_id=ACCESS_KEY,
                     aws_secret_access_key=SECRET_KEY,
                     verify=False,
                     region_name='ap-south-1')

# client is used to connect with different AWS service

client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                     aws_secret_access_key=SECRET_KEY,
                     verify=False,
                     region_name='ap-south-1')

#iam is used to connect with Access Manage User

iam = boto3.client('iam', aws_access_key_id=ACCESS_KEY,
                     aws_secret_access_key=SECRET_KEY,
                     verify=False,
                     region_name='ap-south-1')

redshift = boto3.client('redshift', aws_access_key_id=ACCESS_KEY,
                     aws_secret_access_key=SECRET_KEY,
                     verify=False,
                     region_name='ap-south-1')

# To get connection with the Postgres database which is on premise database
hook = PostgresHook(postgres_conn_id="postgres_localhost")
conn = hook.get_conn()
cursor = conn.cursor()
#comment add

# Declaration of global variables
#All the variables names are being stored in config file
DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


source_bucket_name = 'source-bucket5500'
destination_bucket_name = 'destination-bucket5500'

# configure arguments for dag

#default arguments to be given to the script that runs the automate pipeline
default_args = {
    'owner': 'airflow',
    'retries': 4, # number of times it will run after unsuccessful run
    'retry_delay': timedelta(minutes=5)
}


def store(**kwargs):
    # to store the files to AWS S3 bucket
    # load all the files to the landing area i.e. source bucket in AWS server
    file_path = kwargs['filename']
    key = kwargs['key']
    with open(file_path, 'rb') as f:
        # uploading file by key name
        client.upload_fileobj(f, source_bucket_name, key)


# convert database file to .csv file by appending data row-wise
def postgres_to_s3():
    # get all the data from table and append it to txt file with ',' as delimiter
    # get all the data from database table in cursor object
    cursor.execute("select * from russia_videos")

    # convert the database rows and columns to csv data
    with open('/opt/finalDEproject/data/russia_videos.txt', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()

    # upload txt file as .csv file to AWS S3 bucket
    with open('/opt/finalDEproject/data/russia_videos.txt', 'rb') as f:
        client.upload_fileobj(f, source_bucket_name, 'RU_videos.csv')


def transform_all_files():
    # connect to s3 using boto3 package
    session = boto3.Session(aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
    s3 = session.resource('s3', verify=False)
    my_bucket = s3.Bucket('source-bucket5500')

    # traverse all the object file stored in the source bucket

    for my_bucket_object in my_bucket.objects.all():

        # finding the type of the files
        if '.csv' in my_bucket_object.key:
            # if .csv file
            change_csv.filterData(my_bucket_object.key)
        else:
            change_json.filterData(my_bucket_object.key)


def create_data_warehouse():

    #make a data warehouse only if it is not exists already


    # Step Creating a readshift cluster if similar cluster doesn't exists
    response = redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword="0974Haks",
        IamRoles=[roleArn],
    )

def create_vpc_connection():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    '''vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )'''


def load_category_info():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # connect with redshift cluster
    con = redshift_connector.connect(
        host="data-warehouse5500.c5sfolbhlerh.ap-south-1.redshift.amazonaws.com",
        database=DWH_DB,
        port=5439,
        user=DWH_DB_USER,
        password="0974Haks",
        ssl=False
    )
    con.autocommit = True
    cur = con.cursor()
    # create category_info table in redshift cluster
    cur.execute("""
       create table if NOT EXISTS category_info(
            etag varchar(512),
            id integer, 
            channelId varchar(512),
            title varchar(32),
            PRIMARY KEY (id)
        )
    """)

    # copy Canada category data to table
    cur.execute("""
            copy category_info from 's3://destination-bucket5500/CA_category.json'
            credentials 'aws_iam_role=arn:aws:iam::163976488362:role/redshift-s3-access'
            delimiter ','
            region 'ap-south-1'
            MAXERROR 20000
        """)

    con.commit()


def load_country_info():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # connect with redshift cluster
    con = redshift_connector.connect(
        host="data-warehouse5500.c5sfolbhlerh.ap-south-1.redshift.amazonaws.com",
        database=DWH_DB,
        port=5439,
        user=DWH_DB_USER,
        password="0974Haks",
        ssl=False
    )
    con.autocommit = True
    cur = con.cursor()
    # create a country_info table in redshift cluster
    cur.execute("""
        create table if NOT EXISTS country_info(
            country_code INTEGER,
            country_name VARCHAR(30),
            PRIMARY KEY (country_code)
        )
        
    """)

    # insert country values into table
    cur.execute("""
        insert into country_info values
         (1,'Russia'),
         (2,'USA'),
         (3,'Canada')
    """)


def load_video_info():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # connect with redshift cluster
    con = redshift_connector.connect(
        host="data-warehouse5500.c5sfolbhlerh.ap-south-1.redshift.amazonaws.com",
        database=DWH_DB,
        port=5439,
        user=DWH_DB_USER,
        password="0974Haks",
        ssl=False
    )
    con.autocommit = True
    cur = con.cursor()

    # create videos_info table into redshift cluster
    cur.execute("""
        create table if NOT EXISTS videos_info(
            video_id VARCHAR(32),
            title VARCHAR(256),
            channel_title VARCHAR(32),
            category_id INTEGER,
            views BIGINT,
            likes BIGINT,
            dislikes BIGINT,
            comment_count BIGINT,
            country_code INTEGER,
            PRIMARY KEY (video_id),
            FOREIGN KEY (category_id) references category_info(id),
            FOREIGN KEY (country_code) references country_info(country_code)
        )
    """)

    # copy canada-videos data to the videos_info table
    cur.execute("""
        copy videos_info from 's3://destination-bucket5500/canada_videos.csv'
        credentials 'aws_iam_role=arn:aws:iam::163976488362:role/redshift-s3-access'
        delimiter ','
        region 'ap-south-1'
        CSV
        MAXERROR 20000
    """)

    # copy usa-videos data to the videos_info table
    cur.execute("""
            copy videos_info from 's3://destination-bucket5500/usa_videos.csv'
            credentials 'aws_iam_role=arn:aws:iam::163976488362:role/redshift-s3-access'
            delimiter ','
            region 'ap-south-1'
            CSV
            MAXERROR 20000
        """)


    # copy russia-videos data to the videos_info table
    cur.execute("""
         copy videos_info from 's3://destination-bucket5500/RU_videos.csv'
         credentials 'aws_iam_role=arn:aws:iam::163976488362:role/redshift-s3-access'
         delimiter ','
         region 'ap-south-1'
         CSV         
         MAXERROR 20000
     """)


# initialize dag structure
with DAG(
        default_args=default_args,
        dag_id='youtube-dataset-etl-dag',
        start_date=datetime(2023, 3, 3),
        schedule_interval='@monthly',
        catchup=False
) as dag:
    # list all the task as python operator for each of the files available to transfer it to s3 bucket

    # copy canada category info to  s3 bucket task
    canada_category_info_to_S3 = PythonOperator(
        task_id='canada_category_to_S3',
        python_callable=store,
        op_kwargs={'filename': '/opt/finalDEproject/data/CA_category_id.json', 'key': 'CA_category.json'}
    )

    # copy usa category info to  s3 bucket task
    us_category_info_to_S3 = PythonOperator(
        task_id='us_category_info_to_S3',
        python_callable=store,
        op_kwargs={'filename': '/opt/finalDEproject/data/US_category_id.json', 'key': 'US_category.json'}
    )

    # copy canada videosinfo to s3 bucket task

    canada_video_info_to_S3 = PythonOperator(
        task_id='canada_video_info_to_S3',
        python_callable=store,
        op_kwargs={'filename': '/opt/finalDEproject/data/canada_videos.csv', 'key': 'canada_videos.csv'}
    )

    # copy usa videos info to  s3 bucket task
    usa_video_info_to_S3 = PythonOperator(
        task_id='usa_video_info_to_S3',
        python_callable=store,
        op_kwargs={'filename': '/opt/finalDEproject/data/usa_videos.csv', 'key': 'usa_videos.csv'}
    )

    # copy russia videos info to  s3 bucket task
    russia_video_info_to_S3 = PythonOperator(
        task_id='russia_video_info_to_S3',
        python_callable=postgres_to_s3
    )

    start = DummyOperator(task_id='start')

    # transforming the files with buisness logic
    transform_all_files = PythonOperator(
        task_id='transform_all_files',
        python_callable=transform_all_files,
    )

    # creating a data warehouse task
    create_data_warehouse = PythonOperator(
        task_id='create_data_warehouse',
        python_callable=create_data_warehouse,

    )

    # delaying the task to wait for the cluster to change its state
    delay_python_task = PythonOperator(
        task_id='delay_python_task',
        python_callable=lambda: time.sleep(240)
    )
    create_vpc_connection = PythonOperator(
        task_id='create_vpc_connection',
        python_callable=create_vpc_connection
    )

    # loading task to redshift cluster
    load_video_info = PythonOperator(
        task_id='load_video_info',
        python_callable=load_video_info,
    )

    load_category_info = PythonOperator(
        task_id='load_category_info',
        python_callable=load_category_info,
    )

    load_country_info = PythonOperator(
        task_id='load_country_info',
        python_callable=load_country_info
    )

    all_files_loaded = DummyOperator(task_id='all_files_loaded')

    end = DummyOperator(task_id='end')

    '''
        A complete acyclic graph used for the creating upstream and downstream of the airflow
        It shows the flow of dependency tasks 
    '''
    start >> [canada_category_info_to_S3, us_category_info_to_S3, russia_video_info_to_S3, usa_video_info_to_S3,
              canada_video_info_to_S3] >> all_files_loaded
    all_files_loaded >> transform_all_files >> create_data_warehouse >> delay_python_task
    delay_python_task >> create_vpc_connection>> [load_category_info, load_country_info] >> load_video_info >> end
