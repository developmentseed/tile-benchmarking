import boto3
import fsspec
import json
from pystac import Catalog, Collection, Item, Asset, MediaType
from datetime import datetime
import rio_stac
from pprint import pprint
import concurrent.futures

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("temporal_resolution", choices=["daily", "monthly"], help="Specify the CMIP collection to use (daily or monthly)")
args = parser.parse_args()

if args.temporal_resolution == "daily":
    print("Running STAC generation for daily CMIP6 data...")
    temporal_resolution = "daily"
    anon = True
    s3_path = "s3://nex-gddp-cmip6-cog/daily/GISS-E2-1-G/historical/r1i1p1f2/tas/"
    # Your code for daily frequency goes here
elif args.temporal_resolution == "monthly":
    print("Running STAC generation for monthly CMIP6 data...")
    temporal_resolution = "monthly"
    anon = True
    s3_path = "s3://nex-gddp-cmip6-cog/monthly/CMIP6_ensemble_median/tas/"

fs_read = fsspec.filesystem("s3", anon=anon, skip_instance_cache=False)
fs_write = fsspec.filesystem("")

file_paths = fs_read.glob(f"{s3_path}*")
print(f"{len(file_paths)} discovered from {s3_path}")

# Here we prepend the prefix 's3://', which points to AWS.
if temporal_resolution == "monthly":
    subset_files = sorted(["s3://" + f for f in file_paths if "historical_1950" in f or "historical_1951" in f])
elif temporal_resolution == "daily":
    subset_files = sorted(["s3://" + f for f in file_paths if "_1950_" in f or "_1951_" in f])

print(f"Subseted data to files for 1950 and 1951. {len(subset_files)} files to process.")

# Create the collection
collection_json = json.loads(open(f'cmip6_{temporal_resolution}_stac_collection.json').read())
collection = Collection.from_dict(collection_json)

stac_items_file = f'{collection.id}_stac_items.ndjson'
# clear the file
with open(stac_items_file, 'w') as file:
    pass

def process_item(s3_file):
    print(f"Processing {s3_file}")
    filename = s3_file.split('/')[-1]
    if temporal_resolution == 'monthly':
        input_datetime = filename.split('_')[-1].replace('.tif', '')
        datetime_ = datetime.strptime(input_datetime, '%Y%m')
    elif temporal_resolution == 'daily':
        year, month, day = filename.split('_')[-3:]
        day = day.replace('.tif', '')
        datetime_ = datetime.strptime(f'{year}{month}{day}', '%Y%m%d')    
    with open(stac_items_file, 'a') as f:
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


# For each object, create an Item and add it to the Catalog
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, obj) for obj in subset_files]
