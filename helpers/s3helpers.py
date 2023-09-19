import boto3
import fsspec
import json

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

def list_s3_paths(bucket, prefix, suffixes):
    fs = fsspec.filesystem('s3')
    
    # List to store matching paths
    matching_paths = []
    
    # Use fsspec.walk to iterate through S3 "directories" and "files"
    for path, dirs, files in fs.walk(f's3://{bucket}/{prefix}'):
        for file_or_path in files + dirs:
            full_path = f"{path}/{file_or_path}"
            
            # Check if the path ends with any of the specified suffixes
            if any(full_path.endswith(suffix) for suffix in suffixes):
                matching_paths.append(full_path)

    return matching_paths
