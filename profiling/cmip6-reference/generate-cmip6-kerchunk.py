from tempfile import TemporaryDirectory

import boto3
import fsspec
import ujson
import xarray as xr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from typing import Dict

import argparse

# +
parser = argparse.ArgumentParser()
parser.add_argument(
    "temporal_resolution",
    choices=["daily", "monthly"],
    help="Specify the CMIP collection to use (daily or monthly)")

parser.add_argument(
    "local_or_remote",
    default="local",
    choices=["local", "remote"],
    help="Specify if the kerchunk file should be stored on S3.")

args = parser.parse_args()
# -

# TODO(aimee): Make creating the kerchunk reference optional,
# since kerchunk files may have already been created and we just want to upload them.

if args.temporal_resolution == "daily":
    print("Running kerchunk generation for daily CMIP6 data...")
    temporal_resolution = "daily"
    anon = True
    s3_path = "s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/GISS-E2-1-G/historical/r1i1p1f2/tas/"
    # Your code for daily frequency goes here
elif args.temporal_resolution == "monthly":
    print("Running kerchunk generation for monthly CMIP6 data...")
    temporal_resolution = "monthly"
    anon = False
    s3_path = "s3://climatedashboard-data/cmip6/raw/monthly/CMIP6_ensemble_median/tas/"


# Initiate fsspec filesystems for reading and writing
fs_read = fsspec.filesystem("s3", anon=anon, skip_instance_cache=False)
fs_write = fsspec.filesystem("")

# Retrieve list of available months
files_paths = fs_read.glob(s3_path)
print(f"{len(files_paths)} discovered from {s3_path}")

# Here we prepend the prefix 's3://', which points to AWS.
if temporal_resolution == "monthly":
    subset_files = sorted(["s3://" + f for f in files_paths if ('month_ensemble-median' in f and ("1950" in f or "1951" in f))])
elif temporal_resolution == "daily":
    subset_files = sorted(["s3://" + f for f in files_paths if "1950.nc" in f or "1951.nc" in f])

print(f"{len(subset_files)} file paths were retrieved.")
subset_files

so = dict(mode="rb", anon=anon, default_fill_cache=False, default_cache_type="first")
output_dir = "./"

# We are creating a temporary directory to store the .json reference files
# Alternately, you could write these to cloud storage.
td = TemporaryDirectory()
temp_dir = td.name
print(f"Writing single file references to {temp_dir}")

# Use Kerchunk's `SingleHdf5ToZarr` method to create a `Kerchunk` index from a NetCDF file.
def generate_json_reference(u, temp_dir: str):
    with fs_read.open(u, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, u, inline_threshold=300)
        fname = u.split("/")[-1].strip(".nc")
        outf = f"{fname}.json"
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
        return outf


# Iterate through filelist to generate Kerchunked files. Good use for `Dask`
output_files = []
for single_file in subset_files:
    out_file = generate_json_reference(single_file, temp_dir)
    output_files.append(out_file)

# combine individual references into single consolidated reference
mzz = MultiZarrToZarr(
    output_files,
    remote_protocol='s3',
    remote_options={'anon': anon},
    concat_dims=['time'],
    coo_map={"time": "cf:time"},
    inline_threshold=0
)

multi_kerchunk = mzz.translate()

# Write kerchunk .json record
output_fname = f"combined_{temporal_resolution}_cmip6_kerchunk.json"
with open(f"{output_fname}", "wb") as f:
    print(f"Writing combined kerchunk reference file {output_fname}")
    f.write(ujson.dumps(multi_kerchunk).encode())

# open dataset as zarr object using fsspec reference file system and Xarray
fs = fsspec.filesystem(
    "reference", fo=multi_kerchunk, remote_protocol="s3", remote_options={"anon": anon}
)
m = fs.get_mapper("")

# Check the data
ds = xr.open_dataset(m, engine="zarr", backend_kwargs=dict(consolidated=False))
print(ds)

bucket_name = 'nasa-eodc-data-store'
if args.local_or_remote == 'remote':
    s3 = boto3.client('s3')
    response = s3.upload_file(output_fname, bucket_name, output_fname)
    print(f"Response uploading {output_fname} to {bucket_name} was {response}.")
