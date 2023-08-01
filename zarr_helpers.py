import xarray as xr
import numpy as np
import traceback

def open_dataset(dataset_url, reference: bool = False, anon: bool = True, multiscale: bool = False, z: int = 0):
    ds = None
    try:
        if reference:
            backend_kwargs = {
                'consolidated': False,
                'storage_options': {
                    'fo': dataset_url,
                    'remote_options': {'anon': anon},
                    'remote_protocol': 's3'
                }
            }
            ds = xr.open_dataset("reference://", engine='zarr', backend_kwargs=backend_kwargs)
        else:
            xr_open_args = { 'consolidated': True }
            if multiscale == True:
                xr_open_args["group"] = z
            ds = xr.open_dataset(dataset_url, engine='zarr',  **xr_open_args)
    except Exception as e:
        import pdb; pdb.set_trace();
        print(f"Failed to open {dataset_url} with error {e}")
        traceback.print_exc()
    return ds

def get_dataset_specs(source: str, ds: xr.Dataset):
    shape = dict(zip(ds.dims, ds.shape))
    lat_resolution = np.diff(ds["lat"].values).mean()
    lon_resolution = np.diff(ds["lon"].values).mean()    
    chunks = ds.encoding.get("chunks", "N/A")
    dtype = ds.encoding.get("dtype", "N/A")
    chunks_dict = dict(zip(ds.dims, chunks))
    chunk_size_mb = "N/A" if chunks is None else (np.prod(chunks) * dtype.itemsize)/1024/1024
    compression = ds.encoding.get("compressor", "N/A")
    # calculate coordinate chunks
    number_coord_chunks = 0
    for key in ds.coords.keys():
        if ds[key].shape != ():
            number_coord_chunks += round(ds[key].shape[0]/ds[key].encoding['chunks'][0])
    return {
        'source': source,
        'shape': shape,
        'lat_resolution': lat_resolution,
        'lon_resolution': lon_resolution,
        'chunk_size_mb': chunk_size_mb,
        'chunks': chunks_dict,
        'dtype': dtype,
        'number_coord_chunks': number_coord_chunks,
        'compression': compression
    }

