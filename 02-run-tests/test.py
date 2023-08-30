import boto3
import csv
from io import StringIO
import time
from datetime import datetime
import os
import s3fs
import sys

def generate_timestamp():
    return datetime.now().strftime('%Y-%m-%d-%H%M%S')

def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        func(self, *args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return execution_time
    return wrapper

class Test:
    ## write a function that runs the run function multiple times and continues to append results to self
    # tile_test = TileTest()
    # different_tiles = gen_different_tiles()
    # tile_test.run_batch(10, different_tiles)
    # tile_test.store_results()
    def __init__(
        self,
        dataset_id: str,
        dataset_url: str,
        variable: str,
        niters: int = 1,
        store_results: bool=True
    ):
        self.name = self.__class__.__name__        
        for key, value in kwargs.items():
            setattr(self, key, value)
        # set dataset specs
        # "TS_array_size": "32MB",
        # "TS_chunks": {
        #     "number_coord_chunks": 3,
        #     "number_of_chunks": 8,
        #     "chunk_size": "4MB",
        #     "dtype": "float64",
        #     "compression": "Blosc(cname='lz4', clevel=5, shuffle=SHUFFLE, blocksize=0)"
        # }        

    @timer_decorator
    def run(self):
        raise NotImplementedError("The run method has not been implemented")

    def store_results(self):
        return "Not implemented"
