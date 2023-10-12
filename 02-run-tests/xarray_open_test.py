from typing import Any, Dict, Optional
import diskcache as dc
import xarray
import s3fs
import fsspec
from fsspec.implementations.cached import WholeFileCacheFileSystem, SimpleCacheFileSystem
import re

def xarray_open_dataset(
    src_path: str,
    group: Optional[Any] = None,
    reference: Optional[bool] = False,
    decode_times: Optional[bool] = False,
    file_system: Optional[fsspec.AbstractFileSystem] = None,
) -> xarray.Dataset:
    """Open dataset."""
    xr_open_args: Dict[str, Any] = {
        "engine": "zarr",
        "decode_coords": "all",
        "decode_times": decode_times,
        "consolidated": True,
        "cache": False
    }
    if group:
        xr_open_args["group"] = group

    if reference:
        if not file_system:
            fs = fsspec.filesystem(
                "reference",
                fo=src_path,
                remote_options={"anon": True},
            )
        else:
            fs = file_system
        src_path = fs.get_mapper("")
        xr_open_args["backend_kwargs"] = {"consolidated": False}
    return xarray.open_dataset(src_path, **xr_open_args)

cache = dc.Cache(directory='./diskcache')
@cache.memoize(tag='diskcache_xarray_open_dataset')
def diskcache_xarray_open_dataset(*args, **kwargs):
    return xarray_open_dataset(*args, **kwargs)

def fsspec_xarray_open_dataset(*args, **kwargs):
    # Setting up fsspec with caching
    src_path = kwargs['src_path']
    match = re.match(r'^(s3|https)', src_path)
    protocol = match.group(0)
    reference = kwargs['reference']
    fsspec_args = {
        'cache_storage': 'fsspec_cache',
        'remote_options': {
            'anon': True
        }
    }
    if protocol == 's3':
        # Configure the S3 filesystem with s3fs
        fsspec_args['target_protocol'] = 's3'
        fs = fsspec.filesystem('filecache', **fsspec_args)
        store = s3fs.S3Map(root=src_path, s3=fs)
    # we don't have a use case for this yet
    elif protocol == 'https' and not reference:
        # Set up the file system with caching
        fsspec_args['target_protocol'] = 'https'        
        fs = fsspec.filesystem('filecache', **fsspec_args)        
        store = fsspec.get_mapper(src_path, fs=fs)
    elif protocol == 'https' and reference:
        fsspec_args['target_protocol'] = 'reference' 
        fsspec_args['target_options'] = {
            'fo': src_path
        }
        fs = fsspec.filesystem('filecache', **fsspec_args)
        kwargs['file_system'] = fs
        store = src_path
    kwargs['src_path'] = store
    return xarray_open_dataset(**kwargs)
