# -*- coding: utf-8 -*-
import argparse
import boto3
import json
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
import helpers.zarr_helpers as zarr_helpers
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader

# Step 2: Merge the dictionaries
sources = json.loads(open('../01-generate-datasets/all-datasets.json', 'r').read())

# remove pyramids and https dataset for now
sources = list(filter(lambda x: 'pyramid' not in x[0], sources.items()))
# Also, skip HTTPS for now
sources = list(filter(lambda x: 'https' not in x[1]['dataset_url'], sources))

def get_arguments():
    parser = argparse.ArgumentParser(description="Set environment for the script.")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Environment to run the script in. Options are 'dev' and 'prod'. Default is 'dev'.")
    args = parser.parse_args()
    return args

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
maxzoom = 6
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

if __name__ == "__main__":
    args = get_arguments()
    
    if args.env == "dev":
        HOST = "https://dev-titiler-xarray.delta-backend.com"
    elif args.env == "prod":
        HOST = "https://prod-titiler-xarray.delta-backend.com"
    
    print(f"Running script on HOST: {HOST}")
    
    # Prepare the CSV file
    csv_file = "zarr_info.csv"

    for idx, dataset in enumerate(sources):
        key, value = dataset
        collection_name = key
        source = value['dataset_url']
        variable = value["variable"]
        reference = value.get("extra_args", {}).get("reference", False)
        multiscale = value.get("extra_args", {}).get("multiscale", False)
        ds = xarray_open_dataset(source, reference=reference)
        bounds = default_bounds
        if not multiscale:
            lat_extent, lon_extent = zarr_helpers.get_lat_lon_extents(ds)
            bounds = [lon_extent[0], lat_extent[0], lon_extent[1], lat_extent[1]]

        array_specs = {
            'collection_name': collection_name,
            'source': source
        }
        array_specs.update(zarr_helpers.get_array_chunk_information(ds, variable=variable, multiscale=multiscale))
        mode = "w" if idx == 0 else "a"
        with open(csv_file, mode, newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=array_specs.keys())
            if idx == 0:
                writer.writeheader()
            writer.writerow(array_specs)
            csvfile.close()

        with open(f"urls/{collection_name}_urls.txt", "w") as f:
            f.write(f"HOST={HOST}\n")
            f.write("PATH=tiles/\n")
            f.write("EXT=.png\n")
            query_string = f"QUERYSTRING=?reference={reference}&variable={variable}&url={source}"
            f.write(f"{query_string}\n")
            rows = 0
            extremas, total_weight = generate_extremas(bounds=bounds)
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
