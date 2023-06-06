# Tiling Code Profiling

These scripts are used to generate test data and for profiling tiling code.

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

## pgSTAC

The `pgstac` directory contains scripts for generating test data for profiling pgSTAC.

To regenerate the test STAC data:

```bash
cd pgstac
python generate_cmip6_items.py
```

1. Seed pgSTAC database with test data

```bash
cd pgstac
docker-compose up database
pypgstac load collections cmip6_stac_collection.json --dsn postgresql://username:password@localhost:5439/postgis --method upsert
pypgstac load items cmip6_stac_items.ndjson --dsn postgresql://username:password@localhost:5439/postgis --method upsert
```

## titiler-xarray

The `cmip6-reference` directory contains the `generate-cmip6-kerchunk.py` script for generating test data for profiling titiler-xarray.

## Profiling

```bash
jupyter notebook 
# open profiling/profile.ipynb and select the venv-profiling kernel
```
