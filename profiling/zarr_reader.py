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
    multiscale: Optional[bool] = False,
    reference: Optional[bool] = False,
    anon: Optional[bool] = True,
    decode_times: Optional[bool] = True,
    **kwargs: Any
):
    print(f"Group is {group}")
    """Open dataset."""
    xr_open_args: Dict[str, Any] = {
        "decode_coords": "all",
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
        import pdb; pdb.set_trace()
        ds = xarray.open_zarr(src_path, **xr_open_args)
    time_to_open = round(t.elapsed * 1000, 2)
    return ds, time_to_open


def get_variable(
    ds: xarray.Dataset,
    variable: str,
    time_slice: Optional[str] = None,
    drop_dim: Optional[str] = None,
) -> xarray.DataArray:
    """Get Xarray variable as DataArray."""
    try:
        ds = ds.rename({"lat": "y", "lon": "x"})
    except Exception as e:
        print(f"Caught exception: {e}")
    if drop_dim:
        dim_to_drop, dim_val = drop_dim.split("=")
        ds = ds.sel({dim_to_drop: dim_val}).drop(dim_to_drop)
    da = ds[variable]

    if (da.x > 180).any():
        # Adjust the longitude coordinates to the -180 to 180 range
        da = da.assign_coords(x=(da.x + 180) % 360 - 180)

        # Sort the dataset by the updated longitude coordinates
        da = da.sortby(da.x)

    # Make sure we have a valid CRS
    crs = da.rio.crs or "epsg:4326"
    da.rio.write_crs(crs, inplace=True)

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
