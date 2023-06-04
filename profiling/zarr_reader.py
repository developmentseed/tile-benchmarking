"""ZarrReader."""

import contextlib
from typing import Any, Dict, List, Optional

import attr
import fsspec
import numpy
import rioxarray
import xarray
from profiler.main import Timer

def xarray_open_dataset(
    src_path: str,
    group: Optional[Any] = None,
    reference: Optional[bool] = False,
    decode_times: Optional[bool] = True,
) -> xarray.Dataset:
    """Open dataset."""
    xr_open_args: Dict[str, Any] = {
        "engine": "zarr",
        "decode_coords": "all",
        "decode_times": decode_times,
        "consolidated": True,
    }
    if group:
        xr_open_args["group"] = group

    if reference:
        fs = fsspec.filesystem(
            "reference",
            fo=src_path,
            remote_options={"anon": True},
        )
        src_path = fs.get_mapper("")
        xr_open_args["backend_kwargs"] = {"consolidated": False}

    with Timer() as t:
        ds = xarray.open_dataset(src_path, **xr_open_args)
    print(f"Time elapsed for xarray.open_datset: {round(t.elapsed * 1000, 2)}")
    return ds


def get_variable(
    ds: xarray.Dataset,
    variable: str,
    time_slice: Optional[str] = None,
    drop_dim: Optional[str] = None,
) -> xarray.DataArray:
    """Get Xarray variable as DataArray."""
    ds = ds.rename({"lat": "y", "lon": "x"})
    if drop_dim:
        dim_to_drop, dim_val = drop_dim.split("=")
        ds = ds.sel({dim_to_drop: dim_val}).drop(dim_to_drop)

    da = ds[variable]

    with Timer() as t:
        if (da.x > 180).any():
            # Adjust the longitude coordinates to the -180 to 180 range
            da = da.assign_coords(x=(da.x + 180) % 360 - 180)

            # Sort the dataset by the updated longitude coordinates
            da = da.sortby(da.x)
    print(f"Time elapsed for adjusting longitudes: {round(t.elapsed * 1000, 2)}")

    # Make sure we have a valid CRS
    with Timer() as t:
        crs = da.rio.crs or "epsg:4326"
        da.rio.write_crs(crs, inplace=True)
    print(f"Time elapsed for writing CRS: {round(t.elapsed * 1000, 2)}") 

    # TODO - address this time_slice issue
    with Timer() as t:
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
    print(f"Time elapsed for time slice: {round(t.elapsed * 1000, 5)}")

    return da
