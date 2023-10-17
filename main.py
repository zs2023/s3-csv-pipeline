import argparse
import os
import boto3
import shutil
import logging
import yaml
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
from pyspark.sql import functions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to setup spark session
def setup_spark():
    logging.info('Staring spark session')
    return SparkSession.builder \
    .appName("S3 Data Processing") \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4') \
    .getOrCreate()

# Function to load config.yaml
def load_config():
    logging.info('Loading config.yaml')
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
    os.environ['AWS_ACCESS_KEY_ID'] = config['aws']['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['aws']['secret_key']

# Function to set aws credentials
def set_aws_credentials(spark):
    logging.info('Setting credentials')
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if aws_access_key and aws_secret_key:
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    else:
        logging.warning('AWS credentials not found in environment variables.')
        
# Function to read data from s3
def read_data_from_s3(spark, s3_path):
    logging.info(f"Processing data from {s3_path}")
    return spark.read.csv(s3_path + "*.csv", header=True)

# Function to remove dupliates
def remove_duplicates(df):
    logging.info('Removing duplicates')
    return df.dropDuplicates(["IMPRESSION_ID", "IMPRESSION_DATETIME"])

# Function to aggregate data
def aggregate_data(df):
    logging.info('Aggregating data')
    return df.withColumn("Hour", hour("IMPRESSION_DATETIME")) \
    .groupBy("CAMPAIGN_ID", "Hour") \
    .agg(functions.sum("IMPRESSION").alias("Total_Impressions"))

# Function to write to temp csv
def write_to_temp_csv(df, temp_path):
    logging.info(f"Writing temp CSV file to {temp_path}")
    df.coalesce(1).write \
        .mode('overwrite') \
        .option("header", "true") \
        .csv(temp_path)

# Function to rename the csv file
def rename_csv(temp_path, target_file):
    logging.info('Renaming csv file')
    part_file = [f for f in os.listdir(temp_path) if f.startswith("part")][0]
    shutil.move(os.path.join(temp_path, part_file), os.path.join(temp_path, target_file))

# Function to upload a file to S3
def upload_file_to_s3(file_name, bucket, target_key):
    logging.info('Uploading csv to the target location')
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket, target_key)

# Function to delete temp folder
def delete_temp_folder(folder_path):
    logging.info('Removing temp folder')
    shutil.rmtree(folder_path)

def main(process_date):
    # Create the path
    bucket_name = 'global-interview-bucket'
    temp_path = "./temp_csv/"
    year, month, day = process_date.split('/')
    initials = "ZS" 
    s3_path = f"s3a://{bucket_name}/{process_date}/"
    target_path = f"results/{process_date}/"
    target_file = f"daily_agg_{year}{month}{day}_{initials}.csv"
    full_path = target_path + target_file
    
    # Setup spark session
    spark = setup_spark()
    
    # Load credentials from yaml
    load_config()
    
    # Set aws credentials
    set_aws_credentials(spark)

    # Read the data from the S3 bucket
    df = read_data_from_s3(spark, s3_path)

    # Remove duplicates based on columns IMPRESSION_ID, IMPRESSION_DATETIME
    df_deduplicated = remove_duplicates(df)
    
    # Aggregate data partition by COMPAIGN_ID and the hour the impression occurred
    df_aggregated = aggregate_data(df_deduplicated)

    # Write the DataFrame to CSV
    write_to_temp_csv(df_aggregated, temp_path)

    # Rename the part file to the final file name
    rename_csv(temp_path, target_file)

    # Upload to S3
    upload_file_to_s3(temp_path + target_file, bucket_name, full_path)
    
    # Delete the folder after exporting the CSV
    delete_temp_folder(temp_path)

if __name__ == "__main__":
    # Add argument for date
    parser = argparse.ArgumentParser(description="Process S3 data for a specific date.")
    parser.add_argument("--date", required=True, type=str, help="Date to process (format: YYYY/MM/DD)")
    args = parser.parse_args()

    # Attempt to parse the date string
    try:
        parsed_date = datetime.strptime(args.date, '%Y/%m/%d')
    except ValueError:
        logging.error("Invalid date format. Please enter the date in YYYY/MM/DD format.")
        exit(1)

    # Check if the string is zero-padded
    if args.date != datetime.strftime(parsed_date, '%Y/%m/%d'):
        logging.error("Date must be zero-padded. Please enter the date in YYYY/MM/DD format.")
        exit(1)
    main(args.date)