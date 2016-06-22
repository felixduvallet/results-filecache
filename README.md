# results-filecache

A python decorator for caching the output of a long computation, 
conditioned on a matching md5 hash.

This is most useful for scientific pipelines where you process some
input data into an intermediate storage, then do stuff on the result.
Using a single decorator, this module stores the output of the
processing step to a pickle file after the first time, and will load it
each subsequent time.

Because data is stored to disk, it is persistent across different runs
of the interpreter. It hides away all the logic of checking if the file
exists, if it contains the correct data, or if you should recompute the
data in the cache (do so by changing the expected hash).

In contrast with other methods that hash the function's input, we check
the checksum of the *resulting* file. This is useful in many cases where
the input isn't actually what matters, for example if the input is a
the filename of the data to parse.

## Quick usage:

```
from results_filecache import results_cache

# Define an expected md5sum for the resulting file.
expected_md5 = '89ae471d2783a5bdaca71cd91d2f3274'
cache_decorator = partial(results_cache.cached_call,
                          expected_hash=expected_md5)
                         
# Wrap your processing code with the decorator. If a cache file exists
# with the correct hash, it will be loaded and returned immediately
# instead of processing. Otherwise, it will process the data and save 
# the result to the cache.
@cache_decorator
def process(x):
    print('Running a big task on: {}'.format(x))
    # Do stuff and return it here.
    return

data = process(x)  # This will do the processing work and stores data.
data = process(x)  # This time, the data will be loaded from file. 

```

Note that you can either call `process()` several times in the same
program, or call it repeatedly on the same input across different runs.

## More usage

See [example.py](results_filecache/example.py).
