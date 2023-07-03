#!/bin/bash

temporal_resolution="$1"
local_or_remote="$2"

# Specify json files based on temporal_resolution
if [ -n "$temporal_resolution" ]; then
  if [ "$temporal_resolution" = "daily" ]; then
    echo "Daily data selected"
    collection_json_file="cmip6_daily_stac_collection.json"
    items_json_file="CMIP6_daily_GISS-E2-1-G_TAS_stac_items.ndjson"
  elif [ "$temporal_resolution" = "monthly" ]; then
    echo "Monthly data selected"
    collection_json_file="cmip6_monthly_stac_collection.json"
    items_json_file="CMIP6_ensemble_monthly_median_TAS_stac_items.ndjson" 
  else
    # Handle other cases
    echo "Invalid temporal resolution option"
  fi
else
  # Handle case when no argument is provided
  echo "No argument provided"
fi


# Specify pgstac connection variables based on local_or_remote
if [ -n "$local_or_remote" ]; then
  if [ "$local_or_remote" = "local" ]; then
    echo "Local storage selected"
    username=username
    password=password
    host=localhost
    dbname=postgis
    port=5439
  elif [ "$local_or_remote" = "remote" ]; then
    echo "Remote storage selected"
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
  else
    # Handle other cases
    echo "Invalid local_or_remote option"
  fi
else
  # Handle case when no argument is provided
  echo "No argument provided"
fi

# echo postgresql://$username:$password@$host:$port/$dbname
echo "Inserting collection from $collection_json_file"
pypgstac load collections $collection_json_file --dsn postgresql://$username:$password@$host:$port/$dbname --method upsert
echo "Inserting items from $items_json_file"
pypgstac load items $items_json_file --dsn postgresql://$username:$password@$host:$port/$dbname --method upsert
