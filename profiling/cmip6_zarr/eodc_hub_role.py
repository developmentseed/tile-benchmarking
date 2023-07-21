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
    return credentials
