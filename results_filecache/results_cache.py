"""

A python decorator for caching the output of a long computation, conditioned on
a matching md5 hash.

This is most useful for scientific pipelines where you process some input data
into an intermediate data structure, then do stuff on the result. Using a single
decorator, this module stores the output of the processing step to a pickle file
after the first time, and will load it each subsequent time.

Basic example:
>>> from functools import partial
>>> cache_decorator = partial(
...      cached_call, expected_hash='7ce738d6b23ecaf840773fb4e7999a0a')
>>> @cache_decorator
... def process(x):
...     print('Computing stuff!')
...     return x
...
>>> process([2, 4, 8])  # This will call process() # doctest:+ELLIPSIS
Could not open file: ...
Hashes did not match ...
Computing stuff!
[2, 4, 8]
>>> process([2, 4, 8])  # This will load from cache.pck # doctest:+ELLIPSIS
Loaded matching data from file (hash = ...)
[2, 4, 8]
>>> import os  # Cleanup doctest
>>> os.remove('cache.pck')


Author: Felix Duvallet <felixd@gmail.com>

"""


import hashlib
from functools import wraps

try:
    import cPickle as pickle
except ImportError:
    import pickle


def _compute_file_md5(filename, block_size=2 ** 20):
    """
    Compute the md5 hash of a file, catching any errors.

    Adapted from http://stackoverflow.com/a/1131255/256798

    :param filename:
    :param block_size:
    :return: string version of hash, or empty string if IOError.
    """

    md5 = hashlib.md5()
    try:
        with open(filename, 'rb') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5.update(data)
            return md5.hexdigest()
    except IOError as e:
        print('Could not open file: {}'.format(e))
        return ''


def _load_if_md5_matches(cache_filename, expected_hash):
    """
    Return contents of a pickle file conditioned on its existence and md5 hash
    match.

    :param cache_filename: full path to file.
    :param expected_hash:
    :return: (data, hash): data is None if file doesn't exist or md5 doesn't
    match.
    """
    actual_md5 = _compute_file_md5(cache_filename)

    if expected_hash != actual_md5:
        return None, actual_md5

    try:
        with open(cache_filename, 'rb') as f:
            data = pickle.load(f)
            return data, actual_md5
    except IOError as e:
        # NOTE: any IOErrors will have been caught by _compute_file_md5.
        print('Could not open file: {}'.format(e))
        return None, actual_md5


def _save(data, cache_filename):
    try:
        with open(cache_filename, 'wb') as f:
            pickle.dump(data, f)
        return True
    except IOError as e:
        print('Could not write cache: {}'.format(e))
        return False


def cached_call(function, cache_filename='cache.pck', expected_hash=None):
    """
    Python decorator to cache the output of a function call based on an expected
    checksum (md5 hash) of the file.

    If the given cache filename exists *and* its checksum matches, the result is
    loaded from file and immediately returned (instead of calling.

    Otherwise, the function is called, and the result is stored.

    :param function The function to call/cache.
    :param cache_filename File name for the cache file.
    :param expected_hash The expected md5 hash for the file (e.g., computed
    using md5sum).

    """

    @wraps(function)
    def load_or_compute(*args, **kwargs):
        # Try to load the data. This returns None if the file doesn't exist or
        # the hash does not match.
        (data, actual_hash) = _load_if_md5_matches(
            cache_filename, expected_hash)

        # Here, we have to call the data computation function, then save the
        # output.
        if not data:
            print(('Hashes did not match (expected {}, got {}). Computing '
                   'result and storing to file.').format(
                expected_hash, actual_hash))
            data = function(*args, **kwargs)

            _save(data, cache_filename)
            md5 = _compute_file_md5(cache_filename)

            print('Saved result to file: {} [{}]'.format(
                cache_filename, md5))

        else:
            print('Loaded matching data from file (hash = {})'.format(
                actual_hash))

        return data

    return load_or_compute

# For an example, see example.py
