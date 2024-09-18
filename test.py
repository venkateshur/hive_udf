import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'DEST_BUCKET', 'START_DATE', 'END_DATE', 'SOURCE_TABLE', 'TARGET_TABLE', 'DATABASE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parab1s passed from Glue job
source_bucket = args['SOURCE_BUCKET']
dest_bucket = args['DEST_BUCKET']
start_date = datetime.strptime(args['START_DATE'], '%Y-%m-%d')
end_date = datetime.strptime(args['END_DATE'], '%Y-%m-%d')
source_table = args['SOURCE_TABLE']
target_table = args['TARGET_TABLE']
database = args['DATABASE']

# Initialize S3 client
s3_client = boto3.client('s3')

def get_folders_within_range(bucket_name, start_date, end_date):
    """
    Get folders in the S3 bucket within the specified date range.
    Folder structure is expected to be in the format
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket_name, Prefix='=')
    folders_in_range = []

    for page in result:
        for content in page.get('Contents', []):
            folder_path = content['Key']
            
            if 'dt=' in folder_path:
                # Extract date from folder name (dt=YYYY-MM-DD)
                folder_date_str = folder_path.split('dt=')[1].split('/')[0]
                folder_date = datetime.strptime(folder_date_str, '%Y-%m-%d')

                # Check if the folder date is within the specified range
                if start_date <= folder_date <= end_date:
                    folders_in_range.append(folder_path)
    
    return folders_in_range

def copy_s3_data(source_bucket, dest_bucket, folders_in_range):
    """
    Copy data from source S3 bucket to destination bucket for the specified folders.
    """
    for folder in folders_in_range:
        copy_source = {
            'Bucket': source_bucket,
            'Key': folder
        }
        s3_client.copy(copy_source, dest_bucket, folder)
        print(f"Copied folder {folder} from {source_bucket} to {dest_bucket}")

def refresh_glue_table_partitions_spark_sql(database, table):
    """
    Use Spark SQL to refresh partitions for the Glue table.
    """
    table_name = f"{database}.{table}"
    spark.sql(f"MSCK REPAIR TABLE {table_name}")
    print(f"Partitions refreshed using Spark SQL for Glue table {table_name}")

def filter_and_overwrite_table(database, source_table, target_table, start_date, end_date):
    """
    Read data from source Glue table, filter based on date range, and overwrite partitions in the target table.
    """
    # Load the Glue table into a Spark DataFrame
    source_df = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=source_table
    ).toDF()

    # Filter based on the range of 'dt'
    filtered_df = source_df.filter(
        (source_df['dt'] >= start_date.strftime('%Y-%m-%d')) &
        (source_df['dt'] <= end_date.strftime('%Y-%m-%d'))
    )

    # Write to the target table with partition overwrite mode
    filtered_df.write.partitionBy('test', 'dt', 'test2') \
        .mode('overwrite') \
        .format('parquet') \
        .saveAsTable(f'{database}.{target_table}')

    print(f"Filtered data written to {target_table} with partition overwrite")

# Step 1: Get the folders within the date range
folders_in_range = get_folders_within_range(source_bucket, start_date, end_date)
print(f"Found folders in range: {folders_in_range}")

# Step 2: Copy the data from source to destination bucket
copy_s3_data(source_bucket, dest_bucket, folders_in_range)

# Step 3: Refresh Glue table partitions using Spark SQL
refresh_glue_table_partitions_spark_sql(database, source_table)

# Step 4: Read the refreshed table, filter data, and overwrite partitions in another table
filter_and_overwrite_table(database, source_table, target_table, start_date, end_date)

# Commit the job
job.commit()
