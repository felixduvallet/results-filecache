import unittest
import os
import shutil
import tempfile

from results_filecache import results_cache


class TestResultsCache(unittest.TestCase):

    def setUp(self):
        self.ref_md5 = '932e32598553b0db91af1196443a67fa'
        self.filepath = os.path.join(os.path.dirname(__file__), 'file.pck')
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_compute_hash(self):
        ret = results_cache._compute_file_md5(self.filepath)
        self.assertEqual(self.ref_md5, ret)

    def test_compute_hash_nonexistent(self):
        fpath = os.path.join(os.path.dirname(__file__), 'not_a_file.pck')
        ret = results_cache._compute_file_md5(fpath)
        self.assertEqual('', ret)

    def test_load_matches(self):
        data = results_cache._load_if_md5_matches(self.filepath, self.ref_md5)
        self.assertIsNotNone(data)
        self.assertEqual('Hello, World!', data)

    def test_load_mismatch_hash(self):
        data = results_cache._load_if_md5_matches(self.filepath, '0')
        self.assertIsNone(data)

    def test_load_no_file(self):
        fpath = os.path.join(os.path.dirname(__file__), 'not_a_file.pck')
        data = results_cache._load_if_md5_matches(fpath, self.ref_md5)
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

    def test_cache(self):

        pass


if __name__ == '__main__':
    unittest.main()
