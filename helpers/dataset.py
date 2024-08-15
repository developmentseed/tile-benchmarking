### Utilities for running tile generation tests on datasets

import earthaccess
import fsspec
import morecantile
import xarray as xr


def load_earthaccess_data(concept_id, cmr_query):
    """Search and open dataset associated with concept id and time range arguments using earthaccess"""
    results = earthaccess.search_data(
        concept_id=concept_id, cloud_hosted=True, temporal=cmr_query["temporal"]
    )
    with fsspec.open(results[0].data_links("direct")[0]) as f:
        ds = xr.open_dataset(f)
    return ds


def get_resolution(ds):
    """Return a rough estimate of the dataset resolution"""
    return max(ds.lat.diff(dim="lat").max().values, ds.lon.diff(dim="lon").max().values)


def get_webmercator_extent(ds):
    """Return the dataset area extent, cropped to the bounds of the WebMercatorQuad"""
    return {
        "lon_extent": [int(ds.lon.min().values), int(ds.lon.max().values)],
        "lat_extent": [
            max(-85, int(ds.lat.min().values)),
            min(85, int(ds.lat.max().values)),
        ],
    }


def get_max_zoom_level(resolution):
    """Return the WebMercatorQuad zoom level associated with the full dataset resolution"""
    tms = morecantile.tms.get("WebMercatorQuad")
    return tms.zoom_for_res(resolution, max_z=30, zoom_level_strategy="auto")
