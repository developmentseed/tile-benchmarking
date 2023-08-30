# tile-benchmarking

Home scripts for dynamic tile benchmarking using [titiler-xarray] and [titiler-pgstac].

# What's here

## 01-generate-datasets

This directory includes notebooks for generating:

* "fake" zarr datasets: A notebook for generating Zarr datasets vary by chunk size (higher and higher spatial resolution for the spatial extent of a chunk) and number of chunks (same chunk size for higher and higher spatial resolutions of the array's spatial extent.
* cmip6 kerchunk datasets: A notebook for generating a kerchunk reference for CMIP6 data.
* cmip6 zarr datasets: A notebook for generating CMIP6 zarr data at varying chunk shapes.

You can skip this directory entirely unless you are interested in how the datasets were generated and / or want to modify them for any reason.

ADD ME: COG data generation

## 02-tests

You can run tests on the datasets generated for this project via:

```bash
test-tiling-code input-sources.json
```

This will time tile generation for everything in `input-sources.csv`

Or you can run tests on a spefific data source:

```bash
test-tiling-code --url foo.zarr --variable bar
```

Additional arguments:

* `--zoom`: an integer to test only one zoom
* `--credentials`: a dictionary of AWS credentials used to make requests to `url`
* `--iters`: number of times to run tests

The output will be a json with more information about the dataset as well as an array of timings.

The timings array will be the same length as the number of iterations.

```json
{
    "dataset_id": "power_901_monthly_meteorology_utc.zarr",
    "source": "s3://power-analysis-ready-datastore/power_901_monthly_meteorology_utc.zarr",
    "variable": "TS",
    "dataset_specs": {
        "TS_array_size": "32MB",
        "TS_chunks": {
            "number_coord_chunks": 3,
            "number_of_chunks": 8,
            "chunk_size": "4MB",
            "dtype": "float64",
            "compression": "Blosc(cname='lz4', clevel=5, shuffle=SHUFFLE, blocksize=0)"
        }
    },
    "timings": [
        // time, extra_args
        [429, {"tile": [0,0,0]}],
        [513, {"tile": [1,1,1]}],
        ...
    ]]
}
```

ADD ME: Run tests against the API
ADD ME: Run COG tiling tests
