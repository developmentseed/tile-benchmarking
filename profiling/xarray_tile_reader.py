from __future__ import annotations

import warnings

import attr
from morecantile import Tile
from rasterio.enums import Resampling
from rasterio.transform import from_bounds

from rio_tiler.errors import TileOutsideBounds
from rio_tiler.io import XarrayReader
from rio_tiler.models import ImageData
from rasterio.enums import Resampling
from profiler.main import Timer, profile
import zarr_reader

try:
    import xarray
except ImportError:  # pragma: nocover
    xarray = None  # type: ignore

try:
    import rioxarray
except ImportError:  # pragma: nocover
    rioxarray = None  # type: ignore


@attr.s
class XarrayTileReader(XarrayReader):
    def tile(
        self,
        tile_x: int,
        tile_y: int,
        tile_z: int,
        tilesize: int = 256,
        resampling_method: Resampling = "nearest",
        auto_expand: bool = True,
        timing_results: dict = {},
    ) -> ImageData:
        """Read a Web Map tile from a dataset.

        Args:
            tile_x (int): Tile's horizontal index.
            tile_y (int): Tile's vertical index.
            tile_z (int): Tile's zoom level index.
            tilesize (int, optional): Output image size. Defaults to `256`.
            resampling_method (WarpResampling, optional): WarpKernel resampling algorithm. Defaults to `nearest`.
            auto_expand (boolean, optional): When True, rioxarray's clip_box will expand clip search if only 1D raster found with clip. When False, will throw `OneDimensionalRaster` error if only 1 x or y data point is found. Defaults to True.

        Returns:
            rio_tiler.models.ImageData: ImageData instance with data, mask and tile spatial info.

        """
        if not self.tile_exists(tile_x, tile_y, tile_z):
            raise TileOutsideBounds(
                f"Tile {tile_z}/{tile_x}/{tile_y} is outside bounds"
            )

        tile_bounds = self.tms.xy_bounds(Tile(x=tile_x, y=tile_y, z=tile_z))

        # Create source array by clipping the xarray dataset to extent of the tile.
        with Timer() as t:
            ds = self.input.rio.clip_box(
                *tile_bounds,
                crs=self.tms.rasterio_crs,
                auto_expand=auto_expand,
            )
        timing_results['rio.clip_box'] = round(t.elapsed * 1000, 2)
        
        with Timer() as t:
            #import pdb; pdb.set_trace()
            ds = ds.rio.reproject(
                self.tms.rasterio_crs,
                shape=(tilesize, tilesize),
                transform=from_bounds(*tile_bounds, height=tilesize, width=tilesize),
                resampling=Resampling[resampling_method],
            )
        timing_results['rio.reproject'] = round(t.elapsed * 1000, 2)

        # Forward valid_min/valid_max to the ImageData object
        minv, maxv = ds.attrs.get("valid_min"), ds.attrs.get("valid_max")
        stats = None
        if minv is not None and maxv is not None:
            stats = ((minv, maxv),) * ds.rio.count

        band_names = [str(band) for d in self._dims for band in self.input[d].values]

        with Timer() as t:
            image_data = ImageData(
                ds.data,
                bounds=tile_bounds,
                crs=self.tms.rasterio_crs,
                dataset_statistics=stats,
                band_names=band_names,
            )
        timing_results['ImageData()'] = round(t.elapsed * 1000, 2)

        return image_data, timing_results
 
@profile(add_to_return=True, cprofile=False, quiet=True, log_library='s3fs')
def tile(src_path: str, x: int, y: int, z: int, *, variable: str, time_slice: str = None, **kwargs: Any):
    timing_results = {}

    dataset_and_time = zarr_reader.xarray_open_dataset(src_path, decode_times=False, **kwargs) # group=z
    dataset, time_to_open = dataset_and_time
    timing_results['time_to_open'] = time_to_open

    with Timer() as t:
        dataarray = zarr_reader.get_variable(dataset, variable=variable, time_slice=time_slice)
    timing_results['dimension_and_array_preprocessing'] = round(t.elapsed * 1000, 2)
        
    with XarrayTileReader(dataarray) as src_dst:
        return src_dst.tile(x, y, z, timing_results=timing_results)
