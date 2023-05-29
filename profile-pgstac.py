from geojson_pydantic import Polygon
import io
from titiler.pgstac import model
from psycopg_pool import ConnectionPool
from titiler.pgstac.mosaic import PGSTACBackend
from psycopg.rows import class_row
import morecantile
from typing import Any, Dict, List, Tuple
from rio_tiler.mosaic import mosaic_reader
from rio_tiler.models import ImageData
from PIL import Image

from profiler.main import profile


def pgstac_search():
    return model.PgSTACSearch(
        collections=["CMIP6_ensemble_median_TAS"],
        datetime="1950-04-01T00:00:00Z",
        bbox=[-180, -90, 180, 90]
    )


def pgstac_metadata():
    return model.Metadata(
        type='mosaic',
        bounds=None,
        minzoom=None,
        maxzoom=None,
        name=None,
        assets=['data'],
        defaults={}
    )


pool = ConnectionPool(conninfo="postgresql://username:password@localhost:5439/postgis") 

"""Create map tile."""
@profile(add_to_return=True, cprofile=True, quiet=True)
def tile(
    tile_x: int,
    tile_y: int,
    tile_z: int,
) -> Tuple[ImageData, List[str]]:

    with pool.connection() as conn:
        with conn.cursor(row_factory=class_row(model.Search)) as cursor:
            cursor.execute(
                "SELECT * FROM search_query(%s, _metadata => %s);",
                (
                    pgstac_search().json(by_alias=True, exclude_none=True),
                    pgstac_metadata().json(exclude_none=True),
                ),
            )
            search_info = cursor.fetchone()
            mosaic_id = search_info.id
    
    backend = PGSTACBackend(pool=pool, input=mosaic_id)
    
    def assets_for_tile(x: int, y: int, z: int) -> List[Dict]:
        """Retrieve assets for tile."""
        bbox = backend.tms.bounds(morecantile.Tile(x, y, z))
        return backend.get_assets(Polygon.from_bounds(*bbox))    
    
    """Get Tile from multiple observation."""
    mosaic_assets = assets_for_tile(
        tile_x,
        tile_y,
        tile_z,
    )

    def _reader(
        item: Dict[str, Any], x: int, y: int, z: int, **kwargs: Any
    ) -> ImageData:
        with backend.reader(item, tms=backend.tms, **backend.reader_options) as src_dst:
            return src_dst.tile(x, y, z, **kwargs)

    return mosaic_reader(mosaic_assets, _reader, tile_x, tile_y, tile_z, **{"assets": ["data"]})  

# +
image_and_assets, logs = tile(0,0,0)
content = image_and_assets[0].render(
    img_format='PNG'
)

im = Image.open(io.BytesIO(content))
im
# -

logs
