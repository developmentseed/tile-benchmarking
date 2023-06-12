from geojson_pydantic import Polygon
import io
from titiler.pgstac import model
from psycopg_pool import ConnectionPool
from psycopg.rows import class_row
from typing import Dict


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


# +
def connection_pool():
    return ConnectionPool(conninfo="postgresql://username:password@localhost:5439/postgis") 

def mosaic_id(search_dict: Dict, pool: ConnectionPool):
    with pool.connection() as conn:
        with conn.cursor(row_factory=class_row(model.Search)) as cursor:
            cursor.execute(
                "SELECT * FROM search_query(%s, _metadata => %s);",
                (
                    model.PgSTACSearch(**search_dict).json(by_alias=True, exclude_none=True),
                    pgstac_metadata().json(exclude_none=True),
                ),
            )
            search_info = cursor.fetchone()
            return search_info.id    


# -


