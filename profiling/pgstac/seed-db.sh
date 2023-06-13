#!/bin/bash
stack_name=eodc-dev-pgSTAC
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
