import random
from test import Test, timer_decorator
import morecantile
tms = morecantile.tms.get("WebMercatorQuad")
import sys; sys.path.append('..')
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader

class TileTest(Test):
    def generate_random_tile(self, z):
        random_lat = random.randint(*self.lat_extent)
        random_lon = random.randint(*self.lon_extent)
        tile = tms.tile(random_lon, random_lat, z)
        return (tile.x, tile.y, tile.z)  

    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]
        
    @timer_decorator
    def run(self, xyz_tile: tuple = (0, 0, 0)):
        with ZarrReader(
            self.dataset_url,
            variable=self.variable,
            reference=self.reference
        ) as src_dst:
            image = src_dst.tile(
                *xyz_tile,
                tilesize=256,
            ) 
