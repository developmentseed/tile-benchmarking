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
        "drop_dim_key": "zlev",
        "drop_dim_value": "0", #must be a string otherwise can't be evaluated as truthy
        "anon": True,
    },
    "s3://yuvipanda-test1/cmr/gpm3imergdl.zarr": {
        "collection_name": "gpm3imergdl",
        "variable": "precipitationCal",
        "rescale": "0,704"
    },
    "s3://nasa-eodc-data-store/365_262_262/CMIP6_daily_GISS-E2-1-G_tas.zarr": {
        "collection_name": "CMIP6_daily_GISS-E2-1-G_tas-365_262_262",
        "variable": "tas",
        "rescale": "200,300",
        "bounds": [-179.875, -59.88, 179.9, 89.88]
    },
    "s3://nasa-eodc-data-store/600_1440_1/CMIP6_daily_GISS-E2-1-G_tas.zarr": {
        "collection_name": "CMIP6_daily_GISS-E2-1-G_tas-600_1440_1",
        "variable": "tas",
        "rescale": "200,300",
        "bounds": [-179.875, -59.88, 179.9, 89.88]
    },
    "s3://nasa-eodc-data-store/600_1440_29/CMIP6_daily_GISS-E2-1-G_tas.zarr": {
        "collection_name": "CMIP6_daily_GISS-E2-1-G_tas-600_1440_29",
        "variable": "tas",
        "rescale": "200,300",
        "bounds": [-179.875, -59.88, 179.9, 89.88]
    },
    "s3://nasa-eodc-data-store/600_1440_1_no-coord-chunks/CMIP6_daily_GISS-E2-1-G_tas.zarr": {
        "collection_name": "CMIP6_daily_GISS-E2-1-G_tas-no-coord-chunks",
        "variable": "tas",
        "rescale": "200,300",
        "bounds": [-179.875, -59.88, 179.9, 89.88]
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
    drop_dim_key = value.get("drop_dim_key", False)
    drop_dim_value = value.get("drop_dim_value", False)
    anon = value.get("anon", True)
    ds = zarr_helpers.open_dataset(source, reference=reference, anon=anon, multiscale=False, z=0)
    ds_specs = zarr_helpers.get_dataset_specs(collection_name, source, variable, ds)
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writerow(ds_specs)
        csvfile.close()

    with open(f"urls/{collection_name}_urls.txt", "w") as f:
        f.write("HOST=https://dev-titiler-xarray.delta-backend.com\n")
        f.write("PATH=tiles/\n")
        f.write("EXT=.png\n")
        query_string = f"QUERYSTRING=?reference={reference}&variable={variable}&rescale={rescale}&url={source}"
        if drop_dim_key and drop_dim_value:
            query_string = f"{query_string}&drop_dim={drop_dim_key}={drop_dim_value}"
        f.write(f"{query_string}\n")
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
