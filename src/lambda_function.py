import boto3
import pandas as pd
from datetime import datetime
import uuid


def append_slash_if_missing(input_string):
    if not input_string.endswith('/'):
        input_string += '/'
    return input_string


def append_table_name(input_string, schema_name, table_name):
    if not input_string.endswith('/'):
        input_string += '/'
    input_string += schema_name + '/'
    input_string += table_name + '/'
    return input_string


def list_objects_in_folder(s3_client, bucket, folder):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        return response.get('Contents', [])
    except Exception as e:
        print(f"Error listing objects in {folder}: {str(e)}")
        return []


def copy_object(s3_client, source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key},
                              Bucket=destination_bucket, Key=destination_key)
        return True
    except Exception as e:
        print(f"Error copying object from {source_key} to {destination_key}: {str(e)}")
        return False


def delete_object(s3_client, bucket, key):
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        print(f"Error deleting object {key}: {str(e)}")
        return False


def getDestinationFolderformat(destination_bucket, destination_folder, ingestion_date):
    destination_folder_appended = append_slash_if_missing(destination_folder)
    destination_folder_partition = getFolderPartitionFormat(ingestion_date)
    return destination_folder_appended + "{schema_name}/{table_name}/" + destination_folder_partition


def getSourceFolderFormat(source_bucket, source_folder):
    source_folder_appended = append_slash_if_missing(source_folder)  # raw/
    return source_folder_appended + "{schema_name}/{table_name}"


def getFolderPartitionFormat(ingestion_date):
    # Split the input date string by '-'
    year, month, day = ingestion_date.split('-')

    # Format the date into the desired format
    return f"year={year}/month={month}/day={day}"


def get_ingestion_request_date(destination_folder_partition):
    global year, month, day
    # Split the string by '/'
    parts = destination_folder_partition.split('/')
    # Initialize variables to store the year, month, and day
    year = None
    month = None
    day = None
    # Iterate through the parts and extract the values
    for part in parts:
        if part.startswith("year="):
            year = int(part.split('=')[1])
        elif part.startswith("month="):
            month = int(part.split('=')[1])
        elif part.startswith("day="):
            day = int(part.split('=')[1])

    return f"{year}-{month}-{day}"


def update_table_load_statistics(ingested_tables, metadata_path, ingestion_date):
    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(ingested_tables)

    # Add an additional ingestion date column
    processedDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    df['IngestionDate'] = ingestion_date
    df['ProcessedDate'] = processedDate

    # Generate a unique ID using UUID
    unique_id = str(uuid.uuid4())
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    metadata_filename = f"{metadata_path}year={year}/month={month}/day={day}/metadata_{unique_id}_{timestamp}.parquet"
    # Save the DataFrame to a Parquet file
    df.to_parquet(metadata_filename, index=False)


def move_files_between_folders(source_bucket, source_folder_format, destination_bucket, destination_folder_format,
                               tables):
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    failed_moves = []  # List to store failed moves

    for table in tables:
        schema_name = table.get('SchemaName')
        table_name = table.get('TableName')

        if not schema_name:
            print(f"Skipping invalid schema entry: {table}")
            continue

        if not table_name:
            print(f"Skipping invalid table entry: {table}")
            continue

        source_folder = source_folder_format.format(schema_name=schema_name, table_name=table_name)
        destination_folder = destination_folder_format.format(schema_name=schema_name, table_name=table_name)

        source_objects = list_objects_in_folder(s3_client, source_bucket, source_folder)

        if not source_objects:
            print(f"No objects found in {source_folder}")
            continue

        for obj in source_objects:
            source_key = obj['Key']

            destination_key = destination_folder + source_key[len(source_folder):]

            if copy_object(s3_client, source_bucket, source_key, destination_bucket, destination_key):
                # Delete the object from the source folder after copying (optional)
                # delete_object(s3_client, source_bucket, source_key)
                print(f"Moved {source_key} to {destination_key}")
            else:
                failed_moves.append((source_bucket, source_key))  # Append to failure list

    # Handle failed moves
    if failed_moves:
        print("Failed to move the following objects:")
        for bucket, key in failed_moves:
            print(f"Source: {bucket}/{key}")


def lambda_handler(event, context):
    print(f"event21: {event}")
    print(f"context: {context}")
    # Extract parameters from the event context
    source_bucket = event.get('source_bucket')
    source_folder = event.get('source_folder')
    source_folder_format = getSourceFolderFormat(source_bucket, source_folder)

    ingestion_date = event.get('ingestion_date')
    ingested_tables = event.get('ingested_tables')

    # Generate the destination folder based on the provided date parameter
    current_date = event.get('current_date')
    destination_bucket = event.get('destination_bucket')
    destination_folder = event.get('destination_folder')
    destination_folder_format = getDestinationFolderformat(destination_bucket,
                                                           destination_folder, ingestion_date)

    try:
        move_files_between_folders(source_bucket, source_folder_format, destination_bucket, destination_folder_format,
                                   ingested_tables)
        table_load_statistics_path = append_slash_if_missing(
            f"s3://{source_bucket}/metadata/loadtype=bulkload/")
        update_table_load_statistics(ingested_tables, table_load_statistics_path, ingestion_date)

        return {
            'statusCode': 200,
            'body': f'Tables  repartitioned.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
