import boto3
from datetime import datetime
import json
import morecantile

import random
from typing import Optional
import sys

sys.path.append("..")
from helpers.profiler import Timer

tms = morecantile.tms.get("WebMercatorQuad")


def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """

    def wrapper(self, *args, **kwargs):
        with Timer() as t:
            func(self, *args, **kwargs)
        return round(t.elapsed * 1000, 2)  # time in ms

    return wrapper


class Test:
    def __init__(
        self,
        dataset_id: str,
        dataset_url: Optional[str] = None,
        niters: int = 1,
        bucket: str = "nasa-eodc-data-store",
        results_directory: str = "test-results",
        variable: Optional[str] = None,
        lat_extent: Optional[list] = None,
        lon_extent: Optional[list] = None,
        extra_args: dict = {},
    ):
        self.test_name = self.__class__.__name__
        # Setting named attributes dynamically
        for key, value in locals().items():
            # FIXME: it still seems to be setting the self attribute
            if key != self:
                setattr(self, key, value)
        del self.__dict__["self"]
        self.timings = []

    @timer_decorator
    def run(self, **kwargs):
        raise NotImplementedError("The run method has not been implemented")

    def generate_random_tile(self, z):
        random_lat = random.randint(*self.lat_extent)
        random_lon = random.randint(*self.lon_extent)
        tile = tms.tile(random_lon, random_lat, z)
        return (tile.x, tile.y, tile.z)

    def run_batch(
        self, static_args: dict = {}, batch_size: int = 1, arguments: list = None
    ):
        """Run a function on a batch of data.

        Args:
            generate_arguments (function): A function to generate arguments to pass to successive run call.
            batch_size (int, optional): The size of each batch. Defaults to 100.
        """
        if arguments is None:
            arguments = self.generate_arguments(batch_size, **static_args)
        else:
            batch_size = len(arguments)
        for i in range(0, batch_size):
            time = self.run(arguments[i])
            self.timings.append([time, arguments[i]])
        return self.timings

    def store_results(self, credentials: dict):
        s3_client = boto3.client("s3", **credentials)

        # Serialize the instance to JSON
        instance_dict = self.__dict__.copy()
        del instance_dict["bucket"]
        del instance_dict["results_directory"]
        # TODO - this is specific to pgstac COGs
        if instance_dict.get("pool"):
            del instance_dict["pool"]

        instance_json = json.dumps(instance_dict)

        # Generate a timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Create an object key based on the timestamp, name, and dataset_id attributes
        object_key = f"{self.results_directory}/{timestamp}_{self.test_name}_{self.dataset_id}.json"

        # Write the JSON data to an S3 bucket
        s3_client.put_object(Body=instance_json, Bucket=self.bucket, Key=object_key)

        print(f"Wrote instance data to s3://{self.bucket}/{object_key}")
        return f"s3://{self.bucket}/{object_key}"
