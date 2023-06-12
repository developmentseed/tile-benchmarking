import boto3
import fsspec
import json
from pystac import Catalog, Collection, Item, Asset, MediaType
from datetime import datetime
import rio_stac
from pprint import pprint

fs_read = fsspec.filesystem("s3", anon=False, skip_instance_cache=False)
fs_write = fsspec.filesystem("")
collection = "monthly"

# Retrieve list of available months
datasets = {
    "monthly": {
        "s3_path": "s3://climatedashboard-data/cmip6/monthly/CMIP6_ensemble_median/tas/"
    },
    "daily": {
        "s3_path": "s3://nex-gddp-cmip6-cog/daily/GISS-E2-1-G/historical/r1i1p1f2/tas/"
    }
}

# Create the collection
collection_json = json.loads(open(f'cmip6_{collection}_stac_collection.json').read())
collection = Collection.from_dict(collection_json)

files_paths = fs_read.glob(f"{datasets[collection]['s3_path']}*")

# Here we prepend the prefix 's3://', which points to AWS.
if collection == "monthly":
    subset_files = sorted(["s3://" + f for f in files_paths if "historical_1950" in f or "historical_1951" in f])
elif collection == "daily":
    subset_files = sorted(["s3://" + f for f in files_paths if "_1950_" in f])

print(subset_files)

# For each object, create an Item and add it to the Catalog
with open(f'cmip6_{collection}_stac_items.ndjson', 'w') as f:
    for s3_file in subset_files:
        # Assume the datetime is now, replace this with your own logic
        filename = s3_file.split('/')[-1]
        if collection.id == 'monthly':
            input_datetime = filename.split('_')[-1].replace('.tif', '')
            datetime_ = datetime.strptime(input_datetime, '%Y%m')
        elif collection.id == 'daily':
            year, month, day = filename.split('_')[-3:]
            day = day.replace('.tif', '')
            datetime_ = datetime.strptime(f'{year}{month}{day}', '%Y%m%d')

        # Create a new Item
        item = rio_stac.create_stac_item(
                id=filename,
                source=s3_file,
                collection=collection.id,
                input_datetime=datetime_,
                with_proj=True,
                with_raster=True,
                asset_name="data",
                asset_roles=["data"],
                asset_media_type="image/tiff; application=geotiff; profile=cloud-optimized"
            )
        f.write(json.dumps(item.to_dict()) + '\n')
