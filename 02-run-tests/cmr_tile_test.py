from tile_test import Test, timer_decorator
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import ZarrReader
from titiler.cmr import __version__ as titiler_cmr_version
from rio_tiler.models import ImageData
import psutil

# Local modules
import sys

sys.path.append("..")
import helpers.eodc_hub_role as eodc_hub_role
from helpers.dataset import (
    load_earthaccess_data,
    get_resolution,
    get_webmercator_extent,
    get_max_zoom_level,
)


def generate_arguments(cmr_test, iterations, max_zoom_level):
    """Return titiler-cmr queries for generating random tile indices over all requested zoom levels"""
    arguments = []
    zoom_levels = range(max_zoom_level)
    for zoom in zoom_levels:
        arguments.extend(cmr_test.generate_arguments(iterations, zoom))
    return arguments


def run_cmr_tile_tests(concept_id, cmr_query, variable, iterations):
    """Generate tiles using titiler-cmr. Returns the URI to the results"""
    ds = load_earthaccess_data(concept_id, cmr_query)
    resolution = get_resolution(ds)
    extent = get_webmercator_extent(ds)
    max_zoom_level = get_max_zoom_level(resolution)
    cmr_test = CMRTileTest(
        dataset_id=concept_id,
        lat_extent=extent["lat_extent"],
        lon_extent=extent["lon_extent"],
        cmr_query=cmr_query,
        variable=variable,
    )
    arguments = generate_arguments(cmr_test, iterations, max_zoom_level)
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

    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]

    @timer_decorator
    def run(self, xyz_tile: tuple[int, int, int], **kwargs):
        return tile(
            *xyz_tile,
            concept_id=self.dataset_id,
            cmr_query=self.cmr_query,
            reader_options=self.reader_options,
            **kwargs,
        )
