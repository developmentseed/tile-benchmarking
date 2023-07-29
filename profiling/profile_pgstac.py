from typing import Any, Callable, Dict, List, Sequence, Tuple, Optional

import boto3
from geojson_pydantic import Polygon
import json
import morecantile
import os
from profiler.main import Timer, profile
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool
from rasterio.crs import CRS
import requests
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

    data, mask = pixel_selection.data#.data, pixel_selection.data.mask
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

def connect_to_database(aws_credentials):
    # set environment variables for rasterio
    if os.environ.get('LOCAL') == 'True':
        pool = ConnectionPool(conninfo="postgresql://username:password@localhost:5439/postgis")
    else:
        # Fetch username, password, protocol and database from secrets
        stack_name = os.environ.get('STACK_NAME', None)
        aws_region = os.environ.get('AWS_REGION', 'us-west-2')
        if stack_name == None:
            raise Exception("Please set a stack name in order to set database credentials from secrets manager")
        cf_client = boto3.client(
            'cloudformation',
            region_name=aws_region,
            aws_access_key_id=aws_credentials['AccessKeyId'],
            aws_secret_access_key=aws_credentials['SecretAccessKey'],
            aws_session_token=aws_credentials['SessionToken']
        )
        response = cf_client.describe_stack_resources(StackName=stack_name)

        # Extract the resources from the response
        resources = response['StackResources']

        # Print the details of each resource
        for resource in resources:
            if 'pgstacdbbootstrappgstacinstancesecret'  in resource['LogicalResourceId']:
                secret_physical_resource_id = resource['PhysicalResourceId']
            if 'pgstacdbSecurityGroup' in resource['LogicalResourceId'] and resource['PhysicalResourceId'].startswith('sg-'):
                sg_physical_resource_id = resource['PhysicalResourceId']


        # Add IP to security group inbound
        public_ip = requests.get("http://checkip.amazonaws.com/").text.strip()
        try:
            ec2 = boto3.client(
                'ec2',
                region_name=aws_region,
                aws_access_key_id=aws_credentials['AccessKeyId'],
                aws_secret_access_key=aws_credentials['SecretAccessKey'],
                aws_session_token=aws_credentials['SessionToken']        
            )
            response = ec2.authorize_security_group_ingress(
                GroupId=sg_physical_resource_id,
                IpProtocol='tcp',
                FromPort=5432,
                ToPort=5432,
                CidrIp=f"{public_ip}/32"  # Assuming the IP address is in CIDR notation (e.g., 192.168.1.1/32)
            )
            print("Inbound rule added successfully.")    
        except Exception as e:
            print("Caught exception: " + str(e))

        secrets_client = boto3.client(
            'secretsmanager',
            region_name=aws_region,
            aws_access_key_id=aws_credentials['AccessKeyId'],
            aws_secret_access_key=aws_credentials['SecretAccessKey'],
            aws_session_token=aws_credentials['SessionToken']
        )
        response = secrets_client.get_secret_value(SecretId=secret_physical_resource_id)
        secret_value = response['SecretString']
        secret_dict = json.loads(secret_value)
        username, password = secret_dict['username'], secret_dict['password']
        host, dbname, port = secret_dict['host'], secret_dict['dbname'], secret_dict['port']
        pool = ConnectionPool(conninfo=f"postgresql://{username}:{password}@{host}:{port}/{dbname}")
        print("Connected to database")
        return pool

"""Create map tile."""

@profile(add_to_return=True, cprofile=True, quiet=True, log_library='rasterio')
def tile(
    pool: ConnectionPool,
    tile_x: int,
    tile_y: int,
    tile_z: int,
    query: Dict,
    quiet: bool = True
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
    if quiet != True:
        print(timings)

    return img, assets
