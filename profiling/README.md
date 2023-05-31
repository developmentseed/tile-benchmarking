# Tiling Code Profiling

These scripts are used to generate test data and for profiling tiling code.

## Environment Setup

Create a virtual environment and install dependencies.

    ```bash
    cd profiling
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    
    # symlink the profiler code
    cd ~/YOUR_PYTHON_SYSTEM_PATH
    ln -s THIS REPO'S PATH/profiling/profiler profiler

    # Return to this directory
    # install the timings branch of rio-tiler and titiler-xarray
    pip install git+https://github.com/abarciauskas-bgse/rio-tiler@ab/add-timings#egg=rio-tiler --force
    pip install git+https://github.com/developmentseed/titiler-xarray@ab/add-timings#egg=titiler-xarray --force
    ```

## pgSTAC

The `pgstac` directory contains scripts for generating test data for profiling pgSTAC.

1. Seed pgSTAC database with test data

```bash
cd ../pgstac
docker-compose up database -d
pypgstac load collections cmip6_stac_collection.json --dsn postgresql://username:password@localhost:5439/postgis --method upsert
pypgstac load items cmip6_stac_items.ndjson --dsn postgresql://username:password@localhost:5439/postgis --method upsert
```

## titiler-xarray

The `titiler-xarray` directory contains scripts for generating test data for profiling titiler-xarray.

## Profiling

```bash
jupyter notebook 
# open profiling/profile.ipynb
```
