from urllib.parse import urlparse
import boto3
import pystac
import xstac
import sys; sys.path.append('..');
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset, ZarrReader
import zarr_helpers

#def generate_summary_for_variable(da: xr.DataArray):

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
    # add number of chunks, number of coordinate chunks, total size and chunk size to summaries under the variable
    summaries = {
        'other': {
            'number_coord_chunks': zarr_helpers.get_number_coord_chunks(ds)
        }
    }    
    variable_keys = set(ds.variables) - set(ds.dims)
    for var in variable_keys:
        summaries['other'][var] = zarr_helpers.get_array_chunk_information(ds[var])
        
    summaries = pystac.summaries.Summaries(summaries=summaries)

    collection = pystac.Collection(
        id=_id,
        extent=extent,
        assets = {'zarr-s3': zarr_asset},
        description='for zarr testing',
        summaries=summaries,
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

