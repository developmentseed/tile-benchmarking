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
        print(f"Failed to open {dataset_url} with error {e}")
        traceback.print_exc()
    return ds

# From https://github.com/carbonplan/cmip6-downscaling/blob/main/cmip6_downscaling/methods/common/utils.py#L189
def calc_auspicious_chunks_dict(
    da: xr.DataArray,
    target_size_bytes: int,
    chunk_dims: tuple = ("lat", "lon")
) -> dict:
    """Figure out a chunk size that, given the size of the dataset, the dimension(s) you want to chunk on
    and the data type, will fit under the target_size. Currently only works for 100mb which
    is the recommended chunk size for dask processing.

    Parameters
    ----------
    da : xr.DataArray
        Dataset or data array you're wanting to chunk
    chunk_dims : tuple, optional
        Dimension(s) you want to chunk along, by default ('lat', 'lon')

    Returns
    -------
    chunks_dict : dict
        Dictionary of chunk sizes with the dimensions not listed in `chunk_dims` being the
        length of that dimension (avoiding the shorthand -1 in order to play nice
        with rechunker)
    """
    if not isinstance(chunk_dims, tuple):
        raise TypeError(
            "Your chunk_dims likely includes one string but needs a comma after it! to be a tuple!"
        )
    dim_sizes = dict(zip(da.dims, da.shape))

    # initialize chunks_dict
    chunks_dict = {}

    # dims not in chunk_dims should be one chunk (length -1), since these ones are going to
    # be contiguous while the dims in chunk_dims will be chunked
    for dim in dim_sizes.keys():
        if dim not in chunk_dims:
            # we'll only add the unchunked dimensions to chunks_dict right now
            # rechunker doesn't like the the shorthand of -1 meaning the full length
            # so we'll always just give it the full length of the dimension
            chunks_dict[dim] = dim_sizes[dim]
    data_bytesize = da.dtype.itemsize
    # calculate the size of the smallest minimum chunk based upon dtype and the
    # length of the unchunked dim(s). chunks_dict currently only has unchunked dims right now
    smallest_size_one_chunk = data_bytesize * np.prod([dim_sizes[dim] for dim in chunks_dict])

    # the dims in chunk_dims should be of an array size (as in number of elements in the array)
    # that creates ~100 mb. `perfect_chunk` is the how many of the smallest_size_chunks you can
    # handle at once while still staying below the `target_size_bytes`
    perfect_chunk = target_size_bytes / smallest_size_one_chunk
    # then make reasonable chunk size by rounding up (avoids corner case of it rounding down to 0...)
    # but if the array is oblong it might get big (? is logic right there- might it get small??)
    perfect_chunk_length = int(np.ceil(perfect_chunk ** (1 / len(chunk_dims))))
    for dim in chunk_dims:
        # check that the rounding up as part of the `perfect_chunk_length` calculation
        # didn't make the chunk sizes bigger than the array itself, and if so
        # clip it to that size
        chunks_dict[dim] = min(perfect_chunk_length, dim_sizes[dim])
    return chunks_dict

def get_chunk_size(ds: xr.DataArray): 
    chunks = ds.encoding.get("chunks", "N/A")
    dtype = ds.encoding.get("dtype", "N/A")    
    chunk_size_mb = "N/A" if chunks is None else (np.prod(chunks) * dtype.itemsize)/1024/1024
    return chunks, dtype, chunk_size_mb

def get_dataset_specs(collection_name: str, source: str, variable: str, ds: xr.Dataset):
    ds = ds[variable]
    shape = dict(zip(ds.dims, ds.shape))
    try:
        lat_resolution = np.diff(ds["lat"].values).mean()
        lon_resolution = np.diff(ds["lon"].values).mean()    
    except Exception as e:
        lat_resolution, lon_resolution = 'N/A', 'N/A'
    chunks, dtype, chunk_size_mb = get_chunk_size(ds)
    chunks_dict = dict(zip(ds.dims, chunks))    
    compression = ds.encoding.get("compressor", "N/A")
    # calculate coordinate chunks
    number_coord_chunks = 0
    for key in ds.coords.keys():
        if ds[key].shape != ():
            number_coord_chunks += round(ds[key].shape[0]/ds[key].encoding['chunks'][0])
    return {
        'source': source,
        'collection_name': collection_name,
        'variable': variable,
        'shape': shape,
        'lat_resolution': lat_resolution,
        'lon_resolution': lon_resolution,
        'chunk_size_mb': chunk_size_mb,
        'chunks': chunks_dict,
        'dtype': dtype,
        'number_coord_chunks': number_coord_chunks,
        'compression': compression
    }

