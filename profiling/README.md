# Tiling Code Profiling

These scripts are used to generate test data and for profiling tiling code.

There are few different options for how to run the profiling code:

1. Locally on your machine 
2. On the cloud-hosted jupyterhub instance.

The metadata can also be generated and stored locally or in the AWS cloud.

1. pgSTAC - run the database locally via docker or on the cloud via cdk-pgstac deployment.
2. zarr metadata + kerchunk references - can be referenced locally or via S3.

In all cases, data files themselves are stored in AWS S3.

You can also seed data for either:

1. CMIP6 monthly ensembles and / or
2. CMIP6 daily single models.

The former, monthly ensembles, has publicly available COGs here: https://nex-gddp-cmip6-cog.s3.us-west-2.amazonaws.com/index.html#monthly/CMIP6_ensemble_median/, but the NetCDF files from which those COGs were generated are not available in a public bucket at this time. That is why you will see references to climatedashboard-data in the option for generating a kerchunk reference for monthly ensemble NetCDF files.

## 0. Environment Setup

Create a virtual environment and install dependencies.

Note: Some of the timings require custom versions of rio_tiler modules. So it is important to override the installed versions with those checked into git for this repo.

```bash
cd profiling
# deactivate any existing virtual environment
python -m venv venv-profiling
source venv-profiling/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
# revert changes to rio_tiler
git checkout profiling/venv-profiling/lib/python3.9/site-packages/rio_tiler/
python -m ipykernel install --user --name=venv-profiling
```

## Set default variables

```bash
export temporal_resolution=daily
export storage_location=remote
export model=ACCESS-CM2
export variable=tasmax
```

## Step 1: Seed pgSTAC database with test data

The `cmip6_pgstac` directory contains scripts for generating test data for profiling pgSTAC.

The `generate_cmip6_items.ipynb` generates static json files which will be subsequently used by pypgstac to load as rows into the pgSTAC database.

**If using the `remote` option:** A pgSTAC database is deployed via Github workflows (see the `cdk/` directory and [.github/workflows/deploy.yml](../.github/workflows/deploy.yml)). If using the `remote` option, you will need to have an active AWS session for the same account as the `eodc-dev-pgSTAC` cloudformation stack (currently the SMCE VEDA account) and configured IP based access to the database. If your IP has been added to the database security group and you have an active AWS session with access, you can run the following code to seed the database:

```bash
time \
papermill cmip6_pgstac/generate_cmip6_items.ipynb - \
-p temporal_resolution $temporal_resolution -p storage_location $storage_location \
-p model $model -p variable $variable --log-level DEBUG
```

## Step 3: Generate kerchunk reference for use with `titiler-xarray`

The `cmip6-reference` directory contains the `generate-cmip6-kerchunk.ipynb,py` scripts for generating kerchunk reference files for profiling titiler-xarray.

If using the `remote` option, you will need to have an active AWS session for the same account as the `nasa-eodc-data-store` s3 bucket (currently the SMCE VEDA account).

```bash
time \
papermill cmip6-reference/generate-cmip6-kerchunk.ipynb - \
-p temporal_resolution $temporal_resolution -p storage_location $storage_location \
-p model $model -p variable $variable --log-level DEBUG
```

## Step 4: Generate Zarr store and store on S3

This took about 9 minutes and 18 seconds on my macbook pro (2.6 GHz 6-Core Intel Core i7, 16 GB memory).

```bash
time \
papermill cmip6-zarr/generate-cmip6-zarr.ipynb - \
-p temporal_resolution $temporal_resolution -p storage_location $storage_location \
-p model $model -p variable $variable --log-level DEBUG
```

## Step 4: Profiling

You can run profiling notebook locally:

```bash
# open profiling/profile.ipynb and select the venv-profiling kernel
jupyter notebook 
```

Or you can run profiling notebook on a cloud-hosted jupyterhub instance. The VEDA Program includes a [2i2c JupyterHub](https://nasa-veda.2i2c.cloud/). Find documentation on how to request access [in the VEDA documentation](https://nasa-impact.github.io/veda-docs/services/jupyterhub.html).

Once logged into a jupyterhub instance:

```bash
git clone https://github.com/developmentseed/tile-benchmarking.git
cd tile-benchmarking/
```

Depending on the jupyterhub instance, you may need to install the libraries in requirements.txt.


