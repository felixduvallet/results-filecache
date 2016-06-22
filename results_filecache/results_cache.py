import hashlib
from functools import wraps
import cPickle as pickle


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
    :return: data if file exists and md5 matches, otherwise None
    """
    actual_md5 = _compute_file_md5(cache_filename)
    print('Actual hash for file {}: {}'.format(cache_filename, actual_md5))

    if expected_hash != actual_md5:
        print('Hashes did not match.')
        return None

    print('Hash is a match, loading file.')
    try:
        with open(cache_filename, 'r') as f:
            data = pickle.load(f)
            return data
    except IOError as e:
        # NOTE: any IOErrors will have been caught by _compute_file_md5.
        print('Could not open file: {}'.format(e))
        return None


def _save(data, cache_filename, verbose=False):
    try:
        with open(cache_filename, 'wb') as f:
            pickle.dump(data, f)
            if verbose:
                print('Object saved to {}.'.format(cache_filename))
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
        print('Checking if we already have data... Expected hash = {}'.format(expected_hash))

        # Try to load the data. This returns None if the file doesn't exist or
        # the hash does not match.
        data = _load_if_md5_matches(cache_filename, expected_hash)

        # Here, we have to call the data computation function, then save the
        # output.
        if not data:
            print('Computing data')
            data = function(*args, **kwargs)
            print('Saving result')

            _save(data, cache_filename, verbose=True)

        return data

    return load_or_compute

# For an example, see example.py
