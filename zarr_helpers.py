import xarray as xr
import numpy as np
import s3fs
import traceback
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader

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

def get_dataset_specs_from_directory(zarr_directories: list, variable: str = 'data'):
    datastore_specs = {}
    # Loop through each item and open it with xarray if it's a Zarr store
    for path in zarr_directories:
        try:
            # get the dataset specs
            ds = xarray_open_dataset(f"s3://{path}")
            dataset_name = path.split('/')[-2] + '/' + path.split('/')[-1]
            ds_specs = get_dataset_specs(dataset_name, f"s3://{path}", variable, ds)            
            datastore_specs[dataset_name] = ds_specs
            # product of all dimensions / 
            number_of_chunks = round(np.prod(list(ds_specs['shape'].values())) / np.prod(list(ds_specs['chunks'].values())))
            ds_specs['number_of_chunks'] = number_of_chunks
        except Exception as e:
            # Print an error message if unable to open the Zarr store
            print(f"Could not open {item} as a Zarr store. Error: {e}")
    return datastore_specs

### Tile test helpers
import morecantile
import random
from profiler.main import Timer
import sys; sys.path.append('profiling')
tms = morecantile.tms.get("WebMercatorQuad")

def generate_random_tile(z):
    random_lat = random.randint(-85, 85)
    random_lon = random.randint(-175, 175)
    tile = tms.tile(random_lon, random_lat, z)
    return (tile.x, tile.y, tile.z)

def time_tile_generation(zoom: int, source: str, variable: str, reference: bool = False):
    x, y, z = generate_random_tile(zoom)
    with Timer() as t:
        with ZarrReader(
            source,
            variable=variable,
            reference=reference
        ) as src_dst:
            image = src_dst.tile(
                x,
                y,
                z,
                tilesize=256,
            )
    return round(t.elapsed * 1000, 2) 

def run_tests(datastore_specs: dict, zooms: list, niters: int = 2, variable: str = 'data'):
    for item in datastore_specs.keys():
        datastore_specs[item]['all tile times'] = {}
        datastore_specs[item]['mean tile time'] = {}    
        for zoom in zooms:
            tests_key = f'{zoom} tests'
            datastore_specs[item]['all tile times'][tests_key] = []
            test_results = datastore_specs[item]['all tile times'][tests_key]
            source = datastore_specs[item]['source']
            for i in range(niters):
                tile_time = time_tile_generation(zoom, source, variable)
                test_results.append(tile_time)
            datastore_specs[item]['mean tile time'][zoom] = np.mean(test_results)
    return datastore_specs
