import boto3
import os
import pandas as pd

def csv_to_pandas(file_path):
    df = pd.read_csv(file_path)
    return df

def download_results():
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Specify the bucket name and prefix
    bucket_name = 'nasa-eodc-data-store'
    prefix = 'tile-benchmarking-results/latest/'

    # Create a directory to store the downloaded files
    download_directory = 'downloaded_results'
    os.makedirs(download_directory, exist_ok=True)

    # List all objects in the bucket under the specified prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Loop through each file and download it
    if 'Contents' in response:
        for item in response['Contents']:
            file_name = item['Key'].split('/')[-1]
            download_path = os.path.join(download_directory, file_name)

            print(f"Downloading {item['Key']} to {download_path}")

            s3.download_file(bucket_name, item['Key'], download_path)
    else:
        print("No files found")

    print("Download completed.")
