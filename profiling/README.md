# Tiling Code Profiling

These scripts are used to generate test data and for profiling tiling code.

## Step 1: Setup your environment

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
python3.9 generate_cmip6_items.py
```

### Option 1: Seed a local pgSTAC database with test data

```bash
cd pgstac
docker-compose up database
pypgstac load collections cmip6_stac_collection.json --dsn postgresql://username:password@localhost:5439/postgis --method upsert
pypgstac load items cmip6_stac_items.ndjson --dsn postgresql://username:password@localhost:5439/postgis --method upsert
```

### Option 2: Seed a rempote pgSTAC database with test database

A pgSTAC database is deployed via Github workflows (see the `cdk/` directory and [.github/workflows/deploy.yml](../.github/workflows/deploy.yml)). Access to the database is restricted to certain IPs. If your IP has been added to the database security group, you can run the following code to seed the database:

```bash
cd pgstac
sh ./seed-db.sh
```

## titiler-xarray

The `cmip6-reference` directory contains the `generate-cmip6-kerchunk.py` script for generating test data for profiling titiler-xarray.

## Profiling

```bash
jupyter notebook 
# open profiling/profile.ipynb and select the venv-profiling kernel
```
