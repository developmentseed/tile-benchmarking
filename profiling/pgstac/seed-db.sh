#!/bin/bash
stack_name=arn:aws:cloudformation:us-west-2:444055461661:stack/eodc-dev-pgSTAC/c6e3b0d0-08a9-11ee-b80c-0ab2a5ac006f
region=us-west-2
pgstac_secret_id=$(
    aws cloudformation describe-stack-resources \
    --stack-name $stack_name \
    --output json \
    --region $region \
    --query "StackResources[?contains(LogicalResourceId, 'pgstacdbbootstrappgstacinstancesecret')].PhysicalResourceId" \
    | jq -r '.[0]'
)

# Fetch the secret value
secret_value=$(
    aws secretsmanager get-secret-value \
    --secret-id $pgstac_secret_id \
    --query SecretString --output text \
    --region $region
)

username=$(echo "$secret_value" | sed -n 's/.*"username":"\([^"]*\)".*/\1/p')
password=$(echo "$secret_value" | sed -n 's/.*"password":"\([^"]*\)".*/\1/p')
host=$(echo "$secret_value" | sed -n 's/.*"host":"\([^"]*\)".*/\1/p')
dbname=$(echo "$secret_value" | sed -n 's/.*"dbname":"\([^"]*\)".*/\1/p')

pypgstac load collections cmip6_stac_collection.json --dsn postgresql://$username:$password@$host:5432/$dbname --method upsert
pypgstac load items cmip6_stac_items.ndjson --dsn postgresql://$username:$password@$host:5432/$dbname --method upsert
