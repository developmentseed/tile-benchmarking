# Tiling Code Profiling

These scripts are used to generate test data and for profiling tiling code.

## Environment Setup

Create a virtual environment and install dependencies.

    ```bash
    export WORKING_DIR=`pwd`
    cd profiling
    python -m venv venv-profiling
    source venv-profiling/bin/activate
    pip install -r requirements.txt
    
    # symlink the profiler code
    python -c "import sys; print(sys.path)"
    # A python install path would be something like ~/miniconda3/lib/python3.9
    cd ~/PYTHON_INSTALL_PATH
    ln -s $WORKING_DIR/profiling/profiler profiler

    # Return to this directory
    # install the timings branch of rio-tiler and titiler-xarray
    cd $WORKING_DIR/profiling
    pip install git+https://github.com/abarciauskas-bgse/rio-tiler@ab/add-timings#egg=rio-tiler --force
    pip install git+https://github.com/developmentseed/titiler-xarray@ab/add-timings#egg=titiler-xarray --force
    python -m ipykernel install --user --name=venv-profiling
    ```

## pgSTAC

The `pgstac` directory contains scripts for generating test data for profiling pgSTAC.

1. Seed pgSTAC database with test data

```bash
cd ../pgstac
docker-compose up database
pypgstac load collections cmip6_stac_collection.json --dsn postgresql://username:password@localhost:5439/postgis --method upsert
pypgstac load items cmip6_stac_items.ndjson --dsn postgresql://username:password@localhost:5439/postgis --method upsert
```

## titiler-xarray

The `titiler-xarray` directory contains scripts for generating test data for profiling titiler-xarray.

## Profiling

```bash
jupyter notebook 
# open profiling/profile.ipynb and select the venv-profiling kernel
```
