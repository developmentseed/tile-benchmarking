from tile_test import Test, timer_decorator
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import ZarrReader
from titiler.cmr import __version__ as titiler_cmr_version
from rio_tiler.models import ImageData
from titiler.core.utils import render_image
from titiler.core.resources.enums import ImageType

import pandas as pd
import json

import psutil
import memray
import sys
import fsspec
import subprocess

# Local modules


sys.path.append("..")
import helpers.eodc_hub_role as eodc_hub_role
from helpers.dataset import (
    load_earthaccess_data,
    get_resolution,
    get_webmercator_extent,
    get_max_zoom_level,
)


def generate_memray_summaries(results_dir, output_dir):
    fs = fsspec.filesystem("file")
    files = fs.glob(f"{results_dir}/CMRTileTest*")
    for file in files:
        subprocess.run(
            [
                "memray",
                "stats",
                "--json",
                "--output",
                f"{output_dir}/{file.split('/')[-1]}.json",
                "--force",
                file,
            ]
        )


def generate_memray_flamegraphs(results_dir, output_dir):
    fs = fsspec.filesystem("file")
    files = fs.glob(f"{results_dir}/CMRTileTest*")
    for file in files:
        subprocess.run(
            [
                "memray",
                "flamegraph",
                "--output",
                f"{output_dir}/{file.split('/')[-1]}.html",
                "--force",
                file,
            ]
        )


def get_files(summary_dir):
    fs = fsspec.filesystem("file")
    return fs.glob(f"{summary_dir}/CMRTileTest*")


def process_function_memory_usage(summary_dir):
    df = pd.DataFrame()
    files = get_files(summary_dir)
    for ind, file in enumerate(files):
        metadata_df = get_test_info_from_filename(file).set_index("output_file")
        summary = load_memray_summary(metadata_df.index[0])
        data_df = pd.DataFrame.from_records(summary["top_allocations_by_size"])
        data_df["output_file"] = f"memray-stats/{file.split('/')[-1]}"
        data_df = data_df.set_index("output_file")
        data_df = data_df.join(metadata_df, how="left")
        df = pd.concat([df, data_df])
    df["dataset"] = df["dataset"].apply(lambda x: get_common_name(x))
    df[["method", "location"]] = df["location"].str.split(":", n=1, expand=True)
    df["size (GB)"] = df["size"] * 1e-9
    df = df.drop(columns=["size"])
    df = df.sort_values(by="size (GB)", axis=0, ascending=False)
    df = df.reset_index(drop=True)
    df = df.groupby(by=["zoom", "method", "dataset"]).filter(
        lambda x: x["size (GB)"].mean() > 1.0
    )
    return df


def get_common_name(dataset_name):
    return dataset_name.replace("C1996881146-POCLOUD", "MUR SST").replace(
        "C2723754850-GES", "GPM IMERG"
    )


def get_test_info_from_filename(filepath):
    results_base = filepath.split("/")[-1]
    if "GES_DISC" in results_base:
        df = pd.DataFrame.from_records(
            [results_base.split("_")],
            columns=["test_id", "dataset", "_", "run_id", "x", "y", "zoom"],
        )
        df = df.drop(columns=["_"])
    else:
        df = pd.DataFrame.from_records(
            [results_base.split("_")],
            columns=["test_id", "dataset", "run_id", "x", "y", "zoom"],
        )
    df["dataset"] = df["dataset"].apply(lambda x: get_common_name(x))
    df["run_id"] = df["run_id"].str.strip("run").astype("int")
    df["x"] = df["x"].str.strip("tile")
    df["zoom"] = df["zoom"].str.strip(".json")
    df["output_file"] = f"memray-stats/{results_base}"
    return df


def load_memray_summary(filepath):
    with open(filepath, mode="r") as f:
        data = json.load(f)
    return data


def process_peak_memory_usage(summary_dir):
    df = pd.DataFrame()
    files = get_files(summary_dir)
    for ind, file in enumerate(files):
        metadata_df = get_test_info_from_filename(file)
        summary = load_memray_summary(metadata_df.loc[0, "output_file"])
        metadata_df["peak memory (GB)"] = summary["metadata"]["peak_memory"] * 1e-9
        df = pd.concat([df, metadata_df])
    df["run_id"] = df["run_id"].astype(int)
    df["zoom"] = df["zoom"].astype(int)
    df["peak memory (GB)"] = df["peak memory (GB)"].astype(float)
    return df


def generate_tile_urls(cmr_test, iterations, *, min_zoom_level, max_zoom_level):
    """Return titiler-cmr urls for generating random tile indices over all requested zoom levels"""
    arguments = []
    zoom_levels = range(min_zoom_level, max_zoom_level + 1)
    for zoom in zoom_levels:
        arguments.extend(cmr_test.generate_arguments(iterations, zoom))
    return arguments


def run_cmr_tile_tests(
    dataset_info: dict,
    iterations: int,
    *,
    min_zoom_level: int = 0,
    max_zoom_level: int = None,
):
    """Generate tiles using titiler-cmr. Returns the URI to the results"""
    ds = load_earthaccess_data(dataset_info["concept_id"], dataset_info["cmr_query"])
    resolution = get_resolution(ds)
    extent = get_webmercator_extent(ds)
    if max_zoom_level is None:
        max_zoom_level = get_max_zoom_level(resolution)
    cmr_test = CMRTileTest(
        dataset_id=dataset_info["concept_id"],
        lat_extent=extent["lat_extent"],
        lon_extent=extent["lon_extent"],
        cmr_query=dataset_info["cmr_query"],
        variable=dataset_info["variable"],
        extra_args={
            "rescale": dataset_info["rescale"],
            "colormap_name": dataset_info["colormap_name"],
            "output_format": dataset_info["output_format"],
        },
    )
    arguments = generate_tile_urls(
        cmr_test,
        iterations,
        min_zoom_level=min_zoom_level,
        max_zoom_level=max_zoom_level,
    )
    cmr_test.run_batch(arguments=arguments)
    credentials = eodc_hub_role.fetch_and_set_credentials()
    return cmr_test.store_results(credentials)


def get_size(bytes, suffix="B"):
    """Format bytes."""
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor


def tile(
    tile_x: int,
    tile_y: int,
    tile_z: int,
    *,
    concept_id: str,
    cmr_query: dict,
    reader_options: dict,
    **kwargs,
) -> tuple[ImageData, list[str]]:
    """Create map tile."""
    backend = CMRBackend(reader=ZarrReader, reader_options=reader_options)
    cmr_query["concept_id"] = concept_id
    return backend.tile(tile_x, tile_y, tile_z, cmr_query=cmr_query, **kwargs)


class CMRTileTest(Test):
    def __init__(self, *, cmr_query, **kwargs):
        super().__init__(**kwargs)
        self.cmr_query = cmr_query
        self.reader_options = {"variable": self.variable}
        self.titiler_cmr_version = titiler_cmr_version
        self.physical_cores = psutil.cpu_count(logical=False)
        self.total_cores = psutil.cpu_count(logical=True)
        self.memory = get_size(psutil.virtual_memory().total)
        self.run_counter = 0

    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]

    @timer_decorator
    def run(self, xyz_tile: tuple[int, int, int], **kwargs):
        profile_results = f"results-memray/{self.test_name}_{self.dataset_id}_run{self.run_counter}_tile{xyz_tile[0]}_{xyz_tile[1]}_{xyz_tile[2]}"
        with memray.Tracker(profile_results):
            image, _ = tile(
                *xyz_tile,
                concept_id=self.dataset_id,
                cmr_query=self.cmr_query,
                reader_options=self.reader_options,
                **kwargs,
            )
            if rescale := self.extra_args.get("rescale"):
                image.rescale(rescale)

            output_format = ImageType(self.extra_args.get("output_format", "png"))
            content, media_type = render_image(
                image,
                output_format=output_format,
                colormap=self.extra_args.get("colormap"),
            )
        self.run_counter += 1
        return content
