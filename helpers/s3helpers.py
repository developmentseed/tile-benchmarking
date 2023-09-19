import boto3
import fsspec
import json
import os

def list_s3_files(credentials, bucket_name, s3_prefix):
    s3 = boto3.client('s3', **credentials)
    # List files in the S3 bucket's folder
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    return s3_objects

def download_files(credentials, bucket_name='nasa-eodc-data-store', s3_prefix='tile-benchmarking-results', local_parent_dir='downloaded_results'):
    """
    Download files from an S3 bucket's folder to a local directory with the same name.
    
    Parameters:
        - bucket_name (str): The name of the S3 bucket.
        - s3_prefix (str): The folder in the S3 bucket (S3 prefix).
        - local_parent_dir (str): The local parent directory where files should be downloaded.
    """
    s3 = boto3.client('s3', **credentials)
    # Make sure the local directory exists
    local_folder = os.path.join(local_parent_dir, s3_prefix)
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    
    # List files in the S3 bucket's folder
    s3_objects = list_s3_files(credentials, bucket_name, s3_prefix)
    
    # Check if the bucket is empty
    if 'Contents' not in s3_objects:
        print("No objects are available for this prefix.")
        return
    
    # Download each file to the local directory
    for obj in s3_objects['Contents']:
        s3_key = obj['Key']
        local_file_path = os.path.join(local_parent_dir, s3_key)
        
        # Create local sub-directories if they don't exist
        local_file_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)
        
        # Download file
        s3.download_file(bucket_name, s3_key, local_file_path)
        print(f"Downloaded {s3_key} to {local_file_path}")

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
