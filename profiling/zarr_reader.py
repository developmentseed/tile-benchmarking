"""ZarrReader."""

import contextlib
from typing import Any, Dict, List, Optional

import attr
import fsspec
import numpy
import rioxarray
import xarray
from profiler.main import Timer
from pyproj import CRS
from zarr import errors as zarr_errors

def xarray_open_dataset(
    src_path: str,
    multiscale: Optional[bool] = False,
    reference: Optional[bool] = False,
    anon: Optional[bool] = True,
    decode_times: Optional[bool] = False,
    **kwargs: Any
):
    """Open dataset."""
    xr_open_args: Dict[str, Any] = {
        "decode_coords": False,
        "decode_times": decode_times,
        "consolidated": True
    }
    if multiscale == True:
        xr_open_args["group"] = kwargs['z']

    if reference:
        fs = fsspec.filesystem(
            "reference",
            fo=src_path,
            remote_options={"anon": anon},
        )
        src_path = fs.get_mapper("")
        xr_open_args["consolidated"] = False

    with Timer() as t:
        try:
            ds = xarray.open_zarr(src_path, **xr_open_args)
        except zarr_errors.PathNotFoundError as e:
            # Fallback to the max group
            del xr_open_args['group']
            multiscale_ds = xarray.open_zarr(src_path, **xr_open_args)
            paths = [dataset['path'] for dataset in multiscale_ds.multiscales[0]['datasets']]
            max_group = max(paths)
            ds = xarray.open_zarr(src_path, **xr_open_args, group=max_group)        
        
    time_to_open = round(t.elapsed * 1000, 2)
    return ds, time_to_open


def get_variable(
    ds: xarray.Dataset,
    variable: str,
    time_slice: Optional[str] = None,
    drop_dim: Optional[str] = None,
) -> xarray.DataArray:
    """Get Xarray variable as DataArray."""
    if 'lat' and 'lon' in ds.dims:
        ds = ds.rename({"lat": "y", "lon": "x"})
    if drop_dim:
        dim_to_drop, dim_val = drop_dim.split("=")
        ds = ds.sel({dim_to_drop: dim_val}).drop(dim_to_drop)
    da = ds[variable]
    # Make sure we have a valid CRS
    crs = da.rio.crs or "epsg:4326"
    da.rio.write_crs(crs, inplace=True)

    if da.rio.crs == CRS.from_epsg(4326) and (da.x > 180).any():
        # Adjust the longitude coordinates to the -180 to 180 range
        da = da.assign_coords(x=(da.x + 180) % 360 - 180)

        # Sort the dataset by the updated longitude coordinates
        da = da.sortby(da.x)

    # TODO - address this time_slice issue
    if "time" in da.dims:
        if time_slice:
            time_as_str = time_slice.split("T")[0]
            # TODO(aimee): when do we actually need multiple slices of data?
            # Perhaps if aggregating for coverage?
            # ds = ds[time_slice : time_slice + 1]
            if da["time"].dtype == "O":
                da["time"] = da["time"].astype("datetime64[ns]")
            da = da.sel(
                time=numpy.array(time_as_str, dtype=numpy.datetime64), method="nearest"
            )
        else:
            da = da.isel(time=0)

    return da
