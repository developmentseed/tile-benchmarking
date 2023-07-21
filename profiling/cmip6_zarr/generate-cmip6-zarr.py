# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: venv-profiling
#     language: python
#     name: venv-profiling
# ---

import os
import xarray as xr
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
import s3fs
from cmip6_downscaling.methods.common.utils import calc_auspicious_chunks_dict
import apache_beam as beam
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr

# + tags=["parameters"]
#parameters
model = "GISS-E2-1-G"
variable = "tas"
# -

fs_s3 = s3fs.S3FileSystem(anon=True)
fp = f's3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/historical/r1i1p1f2/{variable}/{variable}_day_{model}_historical_r1i1p1f2_gn_1950.nc'
f = fs_s3.open(fp, mode='rb')
ds = xr.open_dataset(f)

ds

# +
target_chunks = calc_auspicious_chunks_dict(ds['tas'], chunk_dims=('time',))

def format_function(time):
    return f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/historical/r1i1p1f2/tas/tas_day_GISS-E2-1-G_historical_r1i1p1f2_gn_{time}.nc"

years = list(range(1950, 1952))
time_dim = ConcatDim("time", keys=years)

pattern = FilePattern(format_function, time_dim, file_type="netcdf4")
pattern = FilePattern.prune(pattern, nkeep=2)

# -

bucket = 'nasa-eodc-data-store'
target_root = (
    f"s3://{bucket}/{model}"
    "zarr-target"
)
store_name = f"{variable}_day_{model}_historical_r1i1p1f1_gn.zarr"
target_store = os.path.join(target_root, store_name)


transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs={'anon': True})
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name=store_name,
        target_root=target_root,
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
    )
)

with beam.Pipeline() as p:
    p | transforms

ds = xr.open_dataset(target_store, engine="zarr")
ds
