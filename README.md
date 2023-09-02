# tile-benchmarking

Tile benchmarking using [titiler-xarray](https://github.com/developmentseed/titiler-xarray) and [titiler-pgstac](https://github.com/stac-utils/titiler-pgstac).

# Environment Setup

It is recommended to run this project on the [VEDA JupyterHub](https://nasa-veda.2i2c.cloud/) if using datasets generated in `01-generate-datasets/`. See instructions on getting an account on the [VEDA JupyterHub docs page](https://nasa-impact.github.io/veda-docs/services/jupyterhub.html).

It is the intention of this project that it can also be used to benchmark tiling for arbitrary zarr datasets. Examples are forthcoming.

Use the `requirements.txt` to setup a python environment.

# What's here

## [`01-generate-datasets/`](01-generate-datasets/)

This directory includes notebooks for generating:

* "fake" zarr datasets: A notebook for generating Zarr datasets vary by chunk size (higher and higher spatial resolution for the spatial extent of a chunk) and number of chunks (same chunk size for higher and higher spatial resolutions of the array's spatial extent.
* cmip6 kerchunk datasets: A notebook for generating a kerchunk reference for CMIP6 data.
* cmip6 zarr datasets: A notebook for generating CMIP6 zarr data at varying chunk shapes.
* cmip6-pgstac/: A directory with a notebook and instructions on generating STAC for CMIP6 COGs.

You can skip this directory entirely unless you are interested in how the datasets were generated and / or want to modify them for any reason.

## [`02-run-tests/`](02-run-tests/)

See the notebooks in `02-run-tests/` for examples on how to run tests.

At this time, tests exists for timing tile code for xarray and COGs.

TODO: Provide example of how to run a test on a new dataset.
TODO: Run many tests and provide examples of how to report results.
TODO: Add tests to run against API
TODO: Add sub-tile code block timings

## [`03-e2e/`](03-e2e)

Instructions on how to run e2e tests for titiler-xarray with locust and siege.

## [`cdk/`](cdk/)

You can skip this directory unless you are interested in (re)deploying the pgSTAC database for this project.

The cdk/ directory at the root of this repository hosts the code for deploying a pgSTAC database into AWS RDS.

See [eoapi-cdk](https://github.com/developmentseed/eoapi-cdk) if you are interested in the deployment of pgSTAC using CDK.

## [`helpers/`](helpers)

Python functions used more than once in this project are stored here.

## [`titiler_xarray/`](titiler_xarray/)

A git submodule of the [titiler-xarray](https://github.com/developmentseed/titiler-xarray) project in order to profile that codebase.

## Other TODOs:

* Add linting and cleanup unused functions
* Replace helpers/profile_pgstac.py with https://github.com/stac-utils/titiler-pgstac/blob/main/titiler/pgstac/mosaic.py