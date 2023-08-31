import xarray as xr
import numpy as np
import s3fs
import traceback
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader

def get_dataarray_size(da: xr.DataArray):
    dtype = da.encoding.get("dtype", "N/A")
    return np.prod(da.shape) * dtype.itemsize/1024/1024

def get_array_chunk_information(da: xr.DataArray): 
    chunks = da.encoding.get("chunks", "N/A")
    chunks_dict = dict(zip(da.dims, chunks))
    shape_dict = dict(zip(da.dims, da.shape))
    dtype = da.encoding.get("dtype", "N/A")    
    chunk_size_mb = "N/A" if chunks is None else (np.prod(chunks) * dtype.itemsize)/1024/1024
    compression = da.encoding.get("compressor", "N/A")
    # number of chunks in space
    number_of_spatial_chunks = (shape_dict['lat']/chunks_dict['lat']) * (shape_dict['lon']/chunks_dict['lon'])
    return {
        'chunks': chunks_dict,
        'shape_dict': shape_dict,
        'dtype': str(dtype),
        'chunk_size_mb': chunk_size_mb,
        'compression': str(compression),
        'number_of_spatial_chunks': number_of_spatial_chunks
    }

def get_number_coord_chunks(ds: xr.Dataset):
    number_coord_chunks = 0
    for key in ds.coords.keys():
        if ds[key].shape != ():
            number_coord_chunks += round(ds[key].shape[0]/ds[key].encoding['chunks'][0])
    return number_coord_chunks

def get_dataset_specs(collection_name: str, source: str, variable: str, ds: xr.Dataset):
    da = ds[variable]
    shape = dict(zip(da.dims, da.shape))
    try:
        lat_resolution = np.diff(da["lat"].values).mean()
        lon_resolution = np.diff(da["lon"].values).mean()    
    except Exception as e:
        lat_resolution, lon_resolution = 'N/A', 'N/A'
    chunks, dtype, chunk_size_mb, compression, _ = get_array_chunk_information(da).values()
    chunks_dict = dict(zip(ds.dims, chunks))
    # calculate coordinate chunks
    return {
        'source': source,
        'collection_name': collection_name,
        'variable': variable,
        'shape': shape,
        'lat_resolution': lat_resolution,
        'lon_resolution': lon_resolution,
        'chunk_size_mb': chunk_size_mb,
        'chunks': chunks_dict,
        'dataarray_size': get_dataarray_size(da),
        'dtype': dtype,
        'number_coord_chunks': get_number_coord_chunks(ds),
        'compression': compression
    }

def generate_data_store(
    xdim: int,
    ydim: int,
    multiple: int = 0,    
    time_steps: int = 1,
) -> xr.Dataset:
    if multiple:
        # expand grid by multiple
        data_values_per_chunk = ydim * xdim * multiple
        # to maintain the aspect ratio, where we know size == y * x and x = 2y
        ydim = round(np.sqrt(data_values_per_chunk/2))
        xdim = 2*ydim
    data = np.random.random(size=(time_steps, ydim, xdim))

    # Create Xarray datasets with dimensions and coordinates
    ds = xr.Dataset({
        'data': (['time', 'lat', 'lon'], data),
    }, coords={
        'time': np.arange(time_steps),
        'lat': np.linspace(-90, 90, ydim),
        'lon': np.linspace(-180, 180, xdim)
    })
    return ds, xdim, ydim

def generate_multiple_datastores(
    n_multiples: int,
    xdim: int, # starting x dimension
    ydim: int, # starting y dimension
    store_directory: str,
    s3_filesystem: s3fs.S3FileSystem,
    target_chunks: dict = None,
    multiple: int = 2,
) -> None:  
    for n_multiple in range(n_multiples):
        multiple_arg = 0 if n_multiple == 0 else multiple
        ds, xdim, ydim = generate_data_store(xdim, ydim, multiple_arg)

        try:
            if target_chunks:
                ds = ds.chunk(target_chunks)
            else:
                ds = ds.chunk({
                    'time': 1, 
                    'lat': ydim, 
                    'lon': xdim
                })
            path = f'{store_directory}/store_lat{ydim}_lon{xdim}.zarr'
            store = s3fs.S3Map(root=path, s3=s3_filesystem, check=False)
            print(f"Writing to {path}")
            ds.to_zarr(store, mode='w')
        except Exception as e:
            print(e)
