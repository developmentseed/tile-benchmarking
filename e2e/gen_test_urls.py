# -*- coding: utf-8 -*-
import boto3
import fsspec
import math
import random
import click
import morecantile
import numpy as np
import xarray as xr
import zarr
import csv
import os
import sys
sys.path.append('..')
import zarr_helpers

sources = {
    "s3://veda-data-store-staging/EIS/zarr/FWI-GEOS-5-Hourly": {
        "collection_name": "FWI-GEOS-5-Hourly",
        "variable": "GEOS-5_FWI",
        "rescale": "0,40",
        "bounds": [-180, -58, 179.4, 75]
    },
    "s3://power-analysis-ready-datastore/power_901_monthly_meteorology_utc.zarr": {
        "collection_name": "power_901_monthly_meteorology_utc",
        "variable": "TS",
        "rescale": "200,300",
    },
    "s3://cmip6-pds/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G/historical/r2i1p1f1/Amon/tas/gn/v20180827/": {
        "collection_name": "CMIP6_GISS-E2-1-G_historical",
        "variable": "tas",
        "rescale": "200,300",
    },
    "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/pangeo-forge/aws-noaa-oisst-feedstock/aws-noaa-oisst-avhrr-only.zarr/reference.json": {
        "collection_name": "aws-noaa-oisst-avhrr-only",
        "variable": "sst",
        "rescale": "0,40",
        "reference": True,
        "drop_dim": "zlev",
        "drop_dim_value": 0,
        "anon": True,
    },
    "s3://yuvipanda-test1/cmr/gpm3imergdl.zarr": {
        "collection_name": "gpm3imergdl",
        "variable": "precipitationCal",
        "rescale": "0,704",
        "transpose": True
    }
}

def _percentage_split(size, percentages):
    """Freely copied from TileSiege https://github.com/bdon/TileSiege"""
    prv = 0
    cumsum = 0.0
    for zoom, p in percentages.items():
        cumsum += p
        nxt = int(cumsum * size)
        yield zoom, prv, nxt
        prv = nxt

tms = morecantile.tms.get("WebMercatorQuad")

# ##########################################
# INPUTS

minzoom = 0
maxzoom = 7
max_url = 100
default_bounds = [-180, -90, 180, 90]

""
random.seed(3857)

distribution = [
    2,
    2,
    6,
    12,
    16,
    27,
    38,
    41,
    49,
    56,
    72,
    71,
    99,
    135,
    135,
    136,
    102,
    66,
    37,
    6,
    2 
]  # the total distribution...

def generate_extremas(bounds: list[float]):
    w, s, e, n  = bounds
    extremas = {}
    total_weight = 0
    for zoom in range(minzoom, maxzoom + 1):
        total_weight = total_weight + distribution[zoom]
        ul_tile = tms.tile(w, n, zoom, truncate=True)
        lr_tile = tms.tile(e, s, zoom, truncate=True)

        minmax = tms.minmax(zoom)
        extremas[zoom] = {
            "x": {
                "min": max(ul_tile.x, minmax["x"]["min"]),
                "max": min(lr_tile.x, minmax["x"]["max"]),
            },
            "y": {
                "min": max(ul_tile.y, minmax["y"]["min"]),
                "max": min(lr_tile.y, minmax["y"]["max"]),
            },
        }
    return extremas, total_weight


# Prepare the CSV file
csv_file = "zarr_info.csv"
csv_columns = [
    "source",
    "collection_name",
    "variable",
    "shape",
    "lat_resolution",
    "lon_resolution",
    "chunks",
    "chunk_size_mb",
    "number_coord_chunks",
    "dtype",
    "compression"
]

# Write the information to the CSV file
with open(csv_file, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    csvfile.close()

for key, value in sources.items():
    source = key
    variable = value["variable"]
    rescale = value["rescale"]
    collection_name = value["collection_name"]
    reference = value.get("reference", False)
    drop_dim = value.get("drop_dim", False)
    anon = value.get("anon", True)
    transpose = value.get("transpose", False)
    ds = zarr_helpers.open_dataset(source, reference=reference, anon=anon, multiscale=False, z=0)
    ds_specs = zarr_helpers.get_dataset_specs(collection_name, source, variable, ds)
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writerow(ds_specs)
        csvfile.close()

    # Write results to s3
    bucket = 'nasa-eodc-data-store'
    s3dir = 'e2e'
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=os.environ['AWS_SESSION_TOKEN']
    )
    s3.upload_file(csv_file, bucket, f"{s3dir}/{csv_file}")

    with open(f"urls/{collection_name}_urls.txt", "w") as f:
        f.write("HOST=https://dev-titiler-xarray.delta-backend.com\n")
        f.write("PATH=tiles/\n")
        f.write("EXT=.png\n")
        f.write(f"QUERYSTRING=?reference={reference}&variable={variable}&rescale={rescale}&url={source}&transpose={transpose}\n")    
        rows = 0
        extremas, total_weight = generate_extremas(bounds=value.get("bounds", default_bounds))
        for zoom, start, end in _percentage_split(
            max_url,
            {
                zoom: distribution[zoom] / total_weight
                for zoom in range(minzoom, maxzoom + 1)
            },
        ):
            extrema = extremas[zoom]
            rows_for_zoom = end - start
            rows += rows_for_zoom
            for sample in range(rows_for_zoom):
                x = random.randint(extrema["x"]["min"], extrema["x"]["max"])
                y = random.randint(extrema["y"]["min"], extrema["y"]["max"])
                f.write(
                    f"$(HOST)/$(PATH){zoom}/{x}/{y}$(EXT)$(QUERYSTRING)\n"
                )

            if not "quiet":
                p1 = " " if zoom < 10 else ""
                p2 = " " * (len(str(10000)) - len(str(rows_for_zoom)))
                bar = "█" * math.ceil(rows_for_zoom / max_url * 60)
                click.echo(f"{p1}{zoom} | {p2}{rows_for_zoom} {bar}", err=True)
