from profiler.main import profile

from typing import Any
from titiler.xarray.factory import xarray_open_dataset, update_dataset
import morecantile
from rio_tiler.io import XarrayReader

@profile(add_to_return=True, cprofile=True, quiet=True)
def tile(src_path: str, x: int, y: int, z: int, *, variable: str, **kwargs: Any):

    with xarray_open_dataset(
        src_path,
        z=z,
        **kwargs,
    ) as dataset:
        print(dataset)

        dataarray, _ = update_dataset(dataset, variable=variable)
        print(dataarray)        
        
        with XarrayReader(dataarray) as src_dst:
            return src_dst.tile(x, y, z)

# +
import io
from PIL import Image
image_and_assets, logs = tile(
    "titiler-xarray/combined_cmip6_kerchunk.json", 
    0, 
    0, 
    0, 
    reference=True,
    variable="tas",
)


content = image_and_assets.render(
    img_format='PNG'
)

im = Image.open(io.BytesIO(content))
im

# +

print(logs)
