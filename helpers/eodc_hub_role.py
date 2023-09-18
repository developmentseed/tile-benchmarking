import boto3
import os

def fetch_and_set_credentials():
    # Retrieve credentials for the database
    os.environ['STACK_NAME'] = 'eodc-dev-pgSTAC'
    sts_client = boto3.client('sts')
    response = sts_client.get_caller_identity()
    account_id = response['Account']
    response = sts_client.assume_role(RoleArn=f'arn:aws:iam::{account_id}:role/eodc-hub-role',
                                      RoleSessionName='tile-benchmarking')
    credentials = response['Credentials']
    # set 
    os.environ['AWS_ACCESS_KEY_ID'] = credentials['AccessKeyId']
    os.environ['AWS_SECRET_ACCESS_KEY'] = credentials['SecretAccessKey']
    os.environ['AWS_SESSION_TOKEN'] = credentials['SessionToken']  
    return {
        'aws_access_key_id': credentials['AccessKeyId'],
        'aws_secret_access_key': credentials['SecretAccessKey'],
        'aws_session_token': credentials['SessionToken']        
    }

if __name__ == "__main__":
    credentials = fetch_and_set_credentials()
    print(f"""
    export AWS_ACCESS_KEY_ID={credentials['aws_access_key_id']}
    export AWS_SECRET_ACCESS_KEY={credentials['aws_secret_access_key']}
    export AWS_SESSION_TOKEN={credentials['aws_session_token']}    
    """)