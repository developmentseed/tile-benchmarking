#!/bin/bash
model="$1"
variable="$2"

collection_json_file="cmip6_pgstac/CMIP6_daily_${model}_${variable}_collection.json"
items_json_file="cmip6_pgstac/CMIP6_daily_${model}_${variable}_stac_items.ndjson"

# Specify pgstac connection variables based on local_or_remote
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
port=5432

# echo postgresql://$username:$password@$host:$port/$dbname
echo "Inserting collection from $collection_json_file"
pypgstac load collections $collection_json_file --dsn postgresql://$username:$password@$host:$port/$dbname --method upsert
echo "Inserting items from $items_json_file"
pypgstac load items $items_json_file --dsn postgresql://$username:$password@$host:$port/$dbname --method upsert
