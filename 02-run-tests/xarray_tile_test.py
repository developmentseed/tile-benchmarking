from test import Test, timer_decorator
import math
import numpy as np
import sys; sys.path.append('..')
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader
import helpers.zarr_helpers as zarr_helpers

class XarrayTileTest(Test):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if kwargs.get('extra_args', {}).get('reference') == True:
            self.reference = True
        else:
            self.reference = False
        if kwargs.get('extra_args', {}).get('multiscale') == True:
            # TODO(aimee): generate corresponding metrics for a data tree
            self.multiscale = True
        else:
            self.multiscale=False
            ds = xarray_open_dataset(self.dataset_url, reference=self.reference)

            lat_values = ds.lat.values
            lon_values = ds.lon.values
            if (ds.lon > 180).any():
                # Adjust the longitude coordinates to the -180 to 180 range
                lon_values = (ds.lon + 180) % 360 - 180
            self.lat_extent = [math.ceil(np.min(lat_values)), math.floor(np.max(lat_values))]
            self.lon_extent = [math.ceil(np.min(lon_values)), math.floor(np.max(lon_values))]
            self.number_coordinate_chunks = zarr_helpers.get_number_coord_chunks(ds) 
            da = ds[self.variable]
            self.total_array_size = zarr_helpers.get_dataarray_size(da)
            chunk_data = zarr_helpers.get_array_chunk_information(ds, self.variable)
            for key, value in chunk_data.items():
                setattr(self, key, value)
            
    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]
        
    @timer_decorator
    def run(self, xyz_tile: tuple = (0, 0, 0)):
        if self.multiscale:
            group = xyz_tile[2]
        else:
            group = None
        with ZarrReader(
            self.dataset_url,
            variable=self.variable,
            reference=self.reference,
            group=group
        ) as src_dst:
            image = src_dst.tile(
                *xyz_tile,
                tilesize=256,
            ) 
