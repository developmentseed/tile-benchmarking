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

## Environment Setup

Create a virtual environment and install dependencies.

Note: Some of the timings require custom versions of rio_tiler modules. So it is important to override the installed versions with those checked into git for this repo.

    ```bash
    cd profiling
    # deactivate any existing virtual environment
    python3.9 -m venv venv-profiling
    source venv-profiling/bin/activate
    python3.9 -m pip install -U pip
    python3.9 -m pip install -r requirements.txt
    # revert changes to rio_tiler
    git checkout profiling/venv-profiling/lib/python3.9/site-packages/rio_tiler/
    python3.9 -m ipykernel install --user --name=venv-profiling
    ```

## Step 2: Seed pgSTAC database with test data

The `pgstac` directory contains scripts for generating test data for profiling pgSTAC.

To regenerate the CMIP6 STAC metadata. **Note:** you may not need to do this if STAC json files already exist in the `pgstac` directory:

```bash
cd pgstac
python generate_cmip6_items.py <daily|monthly>
```

### Option 1: Seed a local pgSTAC database with test data

If using the `remote` option, you will need to have an active AWS session for the same account as the `eodc-dev-pgSTAC` cloudformation stack (currently the SMCE VEDA account).

```bash
cd pgstac
./seed-db.sh <daily|monthly> <local|remote>
```

### Option 2: Seed a rempote pgSTAC database with test database

A pgSTAC database is deployed via Github workflows (see the `cdk/` directory and [.github/workflows/deploy.yml](../.github/workflows/deploy.yml)). Access to the database is restricted to certain IPs. If your IP has been added to the database security group, you can run the following code to seed the database:

```bash
cd pgstac
sh ./seed-db.sh
```

## titiler-xarray

The `cmip6-reference` directory contains the `generate-cmip6-kerchunk.py` script for generating kerchunk reference files for profiling titiler-xarray.

If using the `remote` option, you will need to have an active AWS session for the same account as the `nasa-eodc-data-store` s3 bucket (currently the SMCE VEDA account).

```bash
cd cmip6-reference
python generate-cmip6-reference.py <daily|monthly> <local|remote>
```

## Profiling

```bash
# open profiling/profile.ipynb and select the venv-profiling kernel
jupyter notebook 
```
