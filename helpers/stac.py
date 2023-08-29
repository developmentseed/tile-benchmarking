from urllib.parse import urlparse
import boto3
import pystac
import xstac
import sys; sys.path.append('..');
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader

def generate_stac(url: str, idstr: str = None, reference: bool = False):
    ds = xarray_open_dataset(url, reference=reference)
    # get extent
    spatial_extent_values = [ds.lon[0].values, ds.lat[0].values, ds.lon[-1].values, ds.lat[-1].values]
    spatial_extent = list(map(int, spatial_extent_values))

    if idstr:
        _id = idstr
    else:
        _id = url.split('/')[-1]
    zarr_asset = pystac.Asset(
        title='zarr',
        href=url,
        media_type='application/vnd+zarr',
        roles=['data'],
    )

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[spatial_extent]),
        temporal=pystac.TemporalExtent([[None, None]])
    )
   
    collection = pystac.Collection(
        id=_id,
        extent=extent,
        assets = {'zarr-s3': zarr_asset},
        description='for zarr testing',
        stac_extensions=['https://stac-extensions.github.io/datacube/v2.0.0/schema.json']
    )
    collection_template = collection.to_dict()
    collection = xstac.xarray_to_stac(
        ds,
        collection_template,
        temporal_dimension="time",
        x_dimension="lon",
        y_dimension="lat",
        # TODO: get this from attributes if possible
        reference_system="4326"
    )
    return collection

