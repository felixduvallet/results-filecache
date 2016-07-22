import sys
import unittest
import os
import shutil
import tempfile
import numpy as np

from results_filecache import results_cache


class TestResultsCache(unittest.TestCase):
    def setUp(self):
        self.ref_md5 = '932e32598553b0db91af1196443a67fa'
        self.filepath = os.path.join(os.path.dirname(__file__), 'file.pck')
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    class C(object):
        # Helper class for wrapping func().
        def __init__(self, ref):
            self.num_calls = 0
            self.ref = ref

        def func(self):
            self.num_calls += 1
            return self.ref

    def test_compute_hash(self):
        ret = results_cache._compute_file_md5(self.filepath)
        self.assertEqual(self.ref_md5, ret)

    def test_compute_hash_nonexistent(self):
        fpath = os.path.join(os.path.dirname(__file__), 'not_a_file.pck')
        ret = results_cache._compute_file_md5(fpath)
        self.assertEqual('', ret)

    def test_load_matches(self):
        (data, _) = results_cache._load_if_md5_matches(self.filepath, self.ref_md5)
        self.assertIsNotNone(data)
        self.assertEqual('Hello, World!', data)

    def test_load_mismatch_hash(self):
        (data, _) = results_cache._load_if_md5_matches(self.filepath, '0')
        self.assertIsNone(data)

    def test_load_no_file(self):
        fpath = os.path.join(os.path.dirname(__file__), 'not_a_file.pck')
        (data, _) = results_cache._load_if_md5_matches(fpath, self.ref_md5)
        self.assertIsNone(data)

    def test_save_good(self):
        filename = os.path.join(self.tmp_dir, 'test.pck')

        data = 'hello, world!'
        ret = results_cache._save(data, filename)
        self.assertTrue(ret)

    def test_save_bad_file(self):
        filename = '/no_rights'
        data = 'hello, world!'
        ret = results_cache._save(data, filename)
        self.assertFalse(ret)

    def test_uncached_class(self):
        # Define a class with a function, call it a few times to make sure we
        # are incrementing the num_calls counter.

        ref = 'Hello, World!'

        c = self.C(ref)
        self.assertEqual(0,  c.num_calls)

        ret = c.func()
        self.assertEqual(1, c.num_calls)
        self.assertEqual(ref, ret)

        ret = c.func()
        self.assertEqual(2, c.num_calls)
        self.assertEqual(ref, ret)

    def test_cached_class(self):
        # Same test as above, but wrap the class's function with the results
        # cache. The first call should happen (and save the results), and the
        # second call will load the data.

        ref = 'Hello, World!'

        c = self.C(ref)

        filename = os.path.join(self.tmp_dir, 'test.pck')

        ref_md5 = '7b72059332de561a5efef5b88b8bac39'
        if sys.version_info[0] >= 3:  # pickle3 results in different md5.
            ref_md5 = 'fcb0b48798cea17c9bfd2634f87be359'
        c.func = results_cache.cached_call(c.func,
                                           cache_filename=filename,
                                           expected_hash=ref_md5)
        ret = c.func()
        self.assertEqual(1, c.num_calls)
        self.assertEqual(ref, ret)

        ret = c.func()
        self.assertEqual(1, c.num_calls)
        self.assertEqual(ref, ret)

    def test_numpy_caching(self):
        # Numpy data exposes a bug where we checked for 'if not data'.
        data = np.array([1, 2, 3])
        c = self.C(data)

        filename = os.path.join(self.tmp_dir, 'test.pck')

        ref_md5 = '00b76b9486d97108ad39484ee341996e'
        if sys.version_info[0] >= 3:  # pickle3 results in different md5.
            ref_md5 = 'TODO'
        c.func = results_cache.cached_call(c.func,
                                           cache_filename=filename,
                                           expected_hash=ref_md5)
        ret = c.func()
        self.assertEqual(1, c.num_calls)
        np.testing.assert_array_equal(data, ret)

        ret = c.func()  # This should return the cached data.
        self.assertEqual(1, c.num_calls)
        np.testing.assert_array_equal(data, ret)


if __name__ == '__main__':
    unittest.main()
