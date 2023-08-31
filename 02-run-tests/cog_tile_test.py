from test import Test, timer_decorator
import sys; sys.path.append('..')
import helpers.profile_pgstac as profile_pgstac
import helpers.eodc_hub_role as eodc_hub_role
import os

GDAL_ENV_VARS = {
    # By default GDAL reads the first 16KB of the file, then if that doesn't contain the entire metadata
    # it makes one more request for the rest of the metadata.
    # In environments where latency is relatively high, AWS S3,
    # it may be beneficial to increase this value depending on the data you expect to read.    
    'GDAL_INGESTED_BYTES_AT_OPEN': '32768',
    # It's much better to set EMPTY_DIR because this prevents GDAL from making a LIST request.
    # LIST requests are made for sidecar files, which does not apply for COGs.  
    'GDAL_DISABLE_READDIR_ON_OPEN': 'EMPTY_DIR',
    # Tells GDAL to merge consecutive range GET requests.    
    'GDAL_HTTP_MERGE_CONSECUTIVE_RANGES': 'YES',
    # When set to YES, this attempts to download multiple range requests in parallel, reusing the same TCP connection. 
    # Note this is only possible when the server supports HTTP2, which many servers don't yet support.
    # There's no downside to setting YES here.    
    'GDAL_HTTP_MULTIPLEX': 'YES',
    'GDAL_HTTP_VERSION': '2',
    # Setting this to TRUE enables GDAL to use an internal caching mechanism. It's recommended (strongly).    
    'VSI_CACHE': 'TRUE'
}

class CogTileTest(Test):
    def set_or_unset_gdal(self, set_vars=True):
        if set_vars:
            for key, value in GDAL_ENV_VARS.items():
                os.environ[key] = value
        else:
            for key, value in GDAL_ENV_VARS.items():
                os.environ.pop(key, None)
        return
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pool = profile_pgstac.connect_to_database(kwargs.get('extra_args', {}).get('credentials'))
        del kwargs['extra_args']['credentials']
        self.set_gdal_vars = kwargs.get('extra_args', {}).get('set_gdal_vars', False)
        self.set_or_unset_gdal(self.set_gdal_vars)
        self.query = kwargs.get('extra_args', {}).get('query')
        self.lat_extent = [-90, 90]
        self.lon_extent = [-180, 180]        
        if self.query == None:
            raise Exception('Please pass a query to profile pgstac.')

    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]
       
    @timer_decorator
    def run(self, xyz_tile: tuple = (0, 0, 0)):
        profile_pgstac.tile(self.pool, *xyz_tile, query=dict(self.query))
