import boto3
import csv
from io import StringIO
import math
import time
from datetime import datetime
import json
import numpy as np
import os
import s3fs
import sys; sys.path.append('..')
import helpers.zarr_helpers as zarr_helpers
from titiler_xarray.titiler.xarray.reader import xarray_open_dataset
from helpers.profiler import Timer

def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """
    def wrapper(self, *args, **kwargs):
        with Timer() as t:
            func(self, *args, **kwargs)
        return round(t.elapsed * 1000, 2) # time in ms
    return wrapper

class Test:
    ## write a function that runs the run function multiple times and continues to append results to self
    # tile_test = TileTest(
    #     dataset_id='cmip6-kerchunk',
    #     dataset_url='',
    #     variable='tas',
    #     extra_dataset_info={'reference': True}
    # )
    # tile_test.run_batch(batch_size=10, zoom=0)
    # tile_test.store_results()
    def __init__(
        self,
        dataset_id: str,
        dataset_url: str,
        variable: str,
        niters: int = 1,
        extra_dataset_info: dict={},
        bucket: str='nasa-eodc-data-store',
        results_directory: str='test-results',
        store_type: str='zarr' # we may include COGs in tests in future
    ):
        self.name = self.__class__.__name__
        # Setting named attributes dynamically
        for key, value in locals().items():
            # FIXME: it still seems to be setting the self attribute
            if key != self:
                setattr(self, key, value)
        del self.__dict__['self']
        self.timings = []
        # set additional dataset information for zarr data
        if store_type == 'zarr':
            if extra_dataset_info.get('reference') == True:
                self.reference = True            
            ds = xarray_open_dataset(dataset_url, reference=self.reference)

            da = ds[variable]
            lat_values = ds.lat.values
            lon_values = ds.lon.values
            if (ds.lon > 180).any():
                # Adjust the longitude coordinates to the -180 to 180 range
                lon_values = (ds.lon + 180) % 360 - 180
            self.lat_extent = [math.ceil(np.min(lat_values)), math.floor(np.max(lat_values))]
            self.lon_extent = [math.ceil(np.min(lon_values)), math.floor(np.max(lon_values))]
            self.array_specs = {
                'number_coordinate_chunks': zarr_helpers.get_number_coord_chunks(ds),
                variable: {
                    'total_array_size': zarr_helpers.get_dataarray_size(da),
                    'chunks': zarr_helpers.get_array_chunk_information(da)
                },
            }

    @timer_decorator
    def run(self, **kwargs):
        raise NotImplementedError("The run method has not been implemented")
        
    def run_batch(self, static_args: dict = {}, batch_size=1):
        """Run a function on a batch of data.

        Args:
            generate_arguments (function): A function to generate arguments to pass to successive run call.
            batch_size (int, optional): The size of each batch. Defaults to 100.
        """
        arguments = self.generate_arguments(batch_size, **static_args)
        for i in range(0, batch_size):
            time = self.run(arguments[i])
            self.timings.append([time, arguments[i]])


    def store_results(self, credentials: dict):
        s3_client = boto3.client('s3', **credentials)

        # Serialize the instance to JSON
        instance_dict = self.__dict__
        instance_json = json.dumps(instance_dict)

        # Generate a timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Create an object key based on the timestamp, name, and dataset_id attributes
        object_key = f"{self.results_directory}/{timestamp}_{self.name}_{self.dataset_id}.json"

        # Write the JSON data to an S3 bucket
        s3_client.put_object(Body=instance_json, Bucket=self.bucket, Key=object_key)

        print(f"Wrote instance data to s3://{self.bucket}/{object_key}")
        return
