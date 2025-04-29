import os
import shutil
import unittest
from aconai.pipelines.data_provider import DataProvider
from aconai.pipelines.data_registry import DataRegistry

class TestableDataProvider(DataProvider):
    def get_schema(self):
        return {
            "namespace": "test.data.provider",
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "test", "type": "int"}
            ]
        }
    
    def get_parameters(self):
        return {}
    
    def get_records(self):
        return [{"test": 1}, {"test": 2}]
    
class BaseTestDataRegistry(unittest.TestCase):
    def setUp(self):
        self.data_dir = "/tmp/data_cache"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        self.registry = DataRegistry(self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

class TestCachedReadNotWritten(BaseTestDataRegistry):

    def test_write_when_not_written(self):
        dp = TestableDataProvider(self.registry)
        records = dp.cached_read()
        expected_file = '/tmp/data_cache/pipelines/test_data_provider/TestableDataProvider/data_0.avro'
        expected_records = [{"test": 1}, {"test": 2}]
        self.assertTrue(os.path.isfile(expected_file))
        self.assertEqual(list(records), expected_records)

class TestCachedReadWritten(BaseTestDataRegistry):

   def test_no_write_when_written(self):
        dp = TestableDataProvider(self.registry)
        dp.cached_read()
        expected_file = '/tmp/data_cache/pipelines/test_data_provider/TestableDataProvider/data_0.avro'
        expected_records = [{"test": 1}, {"test": 2}]
        mtime_before = os.path.getmtime(expected_file)
        records = dp.cached_read()  # Should skip writing
        mtime_after = os.path.getmtime(expected_file)
        self.assertEqual(mtime_before, mtime_after)
        self.assertEqual(list(records), expected_records)

if __name__ == "__main__":
    unittest.main()
