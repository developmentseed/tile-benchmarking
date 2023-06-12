from typing import Any, Callable, Dict, List, Sequence, Tuple, Optional

import morecantile
from geojson_pydantic import Polygon
from profiler.main import Timer, profile
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool
from rasterio.crs import CRS
from rio_tiler.constants import MAX_THREADS
from rio_tiler.errors import EmptyMosaicError, TileOutsideBounds
from rio_tiler.models import ImageData
from rio_tiler.mosaic.methods.defaults import FirstMethod
from rio_tiler.tasks import create_tasks, filter_tasks
from rio_tiler.types import BBox
from rio_tiler.utils import _chunks
from titiler.pgstac import model
from titiler.pgstac.mosaic import PGSTACBackend

def pgstac_search(query: Dict):
    return model.PgSTACSearch(
        collections=query['collections'],
        filter=query,
        bbox=[-180, -90, 180, 90],
        limit=100
    )


def pgstac_metadata():
    return model.Metadata(
        type="mosaic",
        bounds=None,
        minzoom=None,
        maxzoom=None,
        name=None,
        assets=["data"],
        defaults={},
    )


def mosaic_reader(
    mosaic_assets: Sequence,
    reader: Callable[..., ImageData],
    *args: Any,
    chunk_size: Optional[int] = None,
    threads: int = MAX_THREADS,
    allowed_exceptions: Tuple = (TileOutsideBounds,),
    **kwargs,
) -> Tuple[ImageData, List]:
    """Custom version of rio_tiler.mosaic.mosaic_reader."""
    pixel_selection = FirstMethod()

    if not chunk_size:
        chunk_size = threads if threads > 1 else len(mosaic_assets)

    assets_used: List = []
    get_tile_timings: List = []

    crs: Optional[CRS]
    bounds: Optional[BBox]
    band_names: List[str]
  
    # Distribute the assets in chunks (to be processed in parallel)
    # see https://cogeotiff.github.io/rio-tiler/mosaic/#smart-multi-threading
    for chunks in _chunks(mosaic_assets, chunk_size):
        tasks = create_tasks(reader, chunks, threads, *args, **kwargs)
        for img, asset in filter_tasks(
            tasks,
            allowed_exceptions=allowed_exceptions,
        ):
            crs = img.crs
            bounds = img.bounds
            band_names = img.band_names

            # Retrieve the `timing` we set
            get_tile_timings.append(img.metadata.get("timing"))

            assets_used.append(asset)
            pixel_selection.feed(img.as_masked())

            if pixel_selection.is_done:
                data, mask = pixel_selection.data
                return (
                    ImageData(
                        data,
                        mask,
                        assets=assets_used,
                        crs=crs,
                        bounds=bounds,
                        band_names=band_names,
                        # add `get_tile_timings` in the ImageData metadata
                        metadata={"get_tile_timings": get_tile_timings},
                    ),
                    assets_used,
                )

    data, mask = pixel_selection.data
    if data is None:
        raise EmptyMosaicError("Method returned an empty array")

    return (
        ImageData(
            data,
            mask,
            assets=assets_used,
            crs=crs,
            bounds=bounds,
            band_names=band_names,
            metadata={"get_tile_timings": get_tile_timings},
        ),
        assets_used,
    )


pool = ConnectionPool(conninfo="postgresql://username:password@localhost:5439/postgis")

"""Create map tile."""

@profile(add_to_return=True, cprofile=True, quiet=True, log_library='rasterio')
def tile(
    tile_x: int,
    tile_y: int,
    tile_z: int,
    query: Dict,
) -> Tuple[ImageData, List[str]]:
    timings = {}

    with pool.connection() as conn:
        with conn.cursor(row_factory=class_row(model.Search)) as cursor:
            cursor.execute(
                "SELECT * FROM search_query(%s, _metadata => %s);",
                (
                    pgstac_search(query).json(by_alias=True, exclude_none=True),
                    pgstac_metadata().json(exclude_none=True),
                ),
            )
            search_info = cursor.fetchone()
            mosaic_id = search_info.id

    backend = PGSTACBackend(pool=pool, input=mosaic_id)

    def assets_for_tile(x: int, y: int, z: int) -> List[Dict]:
        """Retrieve assets for tile."""
        bbox = backend.tms.bounds(morecantile.Tile(x, y, z))
        return backend.get_assets(Polygon.from_bounds(*bbox), skipcovered=False)

    """Get Tile from multiple observation."""

    # PGSTAC Timing
    with Timer() as t:
        mosaic_assets = assets_for_tile(
            tile_x,
            tile_y,
            tile_z,
        )
    find_assets = t.elapsed
    timings["pgstac-search"] = round(find_assets * 1000, 2)

    def _reader(
        item: Dict[str, Any], x: int, y: int, z: int, **kwargs: Any
    ) -> ImageData:
        # GET TILE Timing
        with Timer() as t:
            with backend.reader(item, tms=backend.tms, **backend.reader_options) as src_dst:
                img = src_dst.tile(x, y, z, **kwargs)
        read_tile_time = round(t.elapsed * 1000, 2)
        img.metadata = {"timing": read_tile_time}

        return img

    # MOSAIC Timing
    with Timer() as t:
        img, assets = mosaic_reader(
            mosaic_assets, _reader, tile_x, tile_y, tile_z, **{"assets": ["data"]}
        )
    timings["get_tile"] = img.metadata["get_tile_timings"]
    timings["mosaic"] = round(t.elapsed * 1000, 2)
    print(timings)

    return img, assets
