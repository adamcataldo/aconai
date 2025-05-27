from datetime import date
import os
import shutil
import unittest
import pandas as pd
from aconai.pipelines.data_registry import DataRegistry
from aconai.pipelines.providers.security_provider import SecurityProvider

class TestSecurityProvider(unittest.TestCase):
    def setUp(self):
        self.data_dir = "/tmp/data_cache"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        self.registry = DataRegistry(self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def test_cached_read(self):
        provider = SecurityProvider(
            self.registry,
            "AAPL",
            date(2020, 1, 1),
            date(2025, 1, 1))
        results = list(provider.cached_read())
        self.assertEqual(len(results), 1)
        df = results[0]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1258, 11))
        column_names = [
            'date',
            'open',
            'high',
            'low',
            'close',
            'adj_open',
            'adj_high',
            'adj_low',
            'adj_close',
            'dividend',
            'split']
        expected_columns = pd.Index(column_names, dtype='object')
        pd.testing.assert_index_equal(df.columns, expected_columns)

if __name__ == '__main__':
    unittest.main()