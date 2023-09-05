import boto3
import fsspec
import json

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
