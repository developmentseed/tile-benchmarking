# +
"""Profiler for titiler-xarray reader."""

import cProfile
import json
import logging
import pstats
import re
import time
from io import StringIO
from typing import Callable

from loguru import logger as log

# This code is copied from marblecutter
#  https://github.com/mojodna/marblecutter/blob/master/marblecutter/stats.py
# License:
# Original work Copyright 2016 Stamen Design
# Modified work Copyright 2016-2017 Seth Fitzsimmons
# Modified work Copyright 2016 American Red Cross
# Modified work Copyright 2016-2017 Humanitarian OpenStreetMap Team
# Modified work Copyright 2017 Mapzen
class Timer(object):
    """Time a code block."""

    def __enter__(self):
        """Start timer."""
        self.start = time.time()
        return self

    def __exit__(self, ty, val, tb):
        """Stop timer."""
        self.end = time.time()
        self.elapsed = self.end - self.start


class Logger():
    def __init__(self, quiet, log_library):
        self.quiet = quiet
        self.log_library = log_library

    def __enter__(self):
        """Start timer."""
        self.log_stream = StringIO()
        self.logger = logging.getLogger(self.log_library)
        self.logger.setLevel(logging.DEBUG)
        self.handler = logging.StreamHandler(self.log_stream)
        self.logger.addHandler(self.handler)        
        return self

    def __exit__(self, ty, val, tb):
        """Stop timer."""
        self.logger.removeHandler(self.handler)
        log_lines = self.log_stream.getvalue().splitlines()
        if not self.quiet:
            print(log_lines)
        self.handler.close()  

def cprofile_list_to_dict(cprofile_list):
    result_dict = {}
    for entry in cprofile_list:
        parts = entry.strip().split("    ")
        if len(parts) >= 5:
            # In case the function is recursive
            # when it's a recursive call the bottom number is the 'primitive' or non-recursive calls.
            # The larger number is the total number of invocations. Debatable which should be included.
            parts[0] = parts[0].split("/")[0]
            ncalls, tottime, percall, cumtime = map(float, parts[:4])
            function = parts[4]
            function_str = re.sub(r'^\d+\.\d+', '', function).strip()
            entry_dict = {
                "ncalls": ncalls,
                "tottime": tottime,
                "percall": percall,
                "cumtime": cumtime,
                "filename:lineno(function)": function_str,
            }
            result_dict[function_str] = entry_dict
    return result_dict        

def profile(
    add_to_return: bool = False,
    quiet: bool = False,
    cprofile: bool = False,
    log_library: str = 's3fs'
):
    """Profiling."""

    def wrapper(func: Callable):
        """Wrap a function."""

        def wrapped_f(*args, **kwargs):
            """Wrapped function."""
            results = {}

            with Logger(quiet=quiet, log_library=log_library) as l:
                with Timer() as t:
                    # cProfile is a simple Python profiling
                    prof = cProfile.Profile()
                    retval = prof.runcall(func, *args, **kwargs)
                    profile_stream = StringIO()
                    ps = pstats.Stats(prof, stream=profile_stream)
                    ps.strip_dirs().sort_stats("time").print_stats()

            if cprofile:
                profile_lines = [p for p in profile_stream.getvalue().splitlines() if p]
                stats_to_print = [
                    p for p in profile_lines[3:] if float(p.split()[1]) > 0.00
                ]
                results["cprofile"] = [profile_lines[2], *stats_to_print]


            if not quiet:
                log.info(json.dumps(results))

            if add_to_return:
                return retval, results

            return retval

        return wrapped_f

    return wrapper
