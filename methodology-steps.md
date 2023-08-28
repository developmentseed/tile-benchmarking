# Step 1: Generate data

Remote datasets
TODO: find 2 more datasets
- external-datasets.csv

CMIP6 data
- generate-cmip6-zarr.ipynb, generate-cmip6-kerchunk.ipynb
- bucket: nasa-eodc-data-store
- directory: cmip6-zarrs

fake data
- generate-fake-data-with-chunks.ipynb
- bucket: nasa-eodc-data-store
- directory: fake-data

TODO: Append to a list of all datasets 

# Step 2: Generate data catalog

TODO:
WIP in 02-generate-stac/main.ipynb
Need to add chunks and write stac and loop through all and store in S3
Write all stac to local
Instructions for how to add a dataset easily

# Step 3: Time tile generation code

Update to use STAC catalog datasets
Correlate with chunk size, number of coordinate chunks and number of total chunks

# Step 4: Time tile generation via API

# Step 5: Estimate time to rechunk

Using rechunker, estimate cost using current infrastructure.

# Step 6: Estimate time to pyramid

Using ndpyramid, estimate cost using current infrastructure.

