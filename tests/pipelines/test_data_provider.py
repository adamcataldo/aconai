import os
import shutil
import unittest
from aconai.pipelines.data_provider import DataProvider
from aconai.pipelines.data_registry import DataRegistry

class TestableDataProvider(DataProvider[object]):
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
    
    def record_as_type(self, record: dict):
        return record
    
class BaseTestDataProvider(unittest.TestCase):
    def setUp(self):
        self.data_dir = "/tmp/data_cache"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        self.registry = DataRegistry(self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

class TestCachedReadNotWritten(BaseTestDataProvider):

    def test_write_when_not_written(self):
        dp = TestableDataProvider(self.registry)
        records = dp.cached_read()
        expected_file = '/tmp/data_cache/pipelines/test_data_provider/TestableDataProvider/data_0.avro'
        expected_records = [{"test": 1}, {"test": 2}]
        self.assertTrue(os.path.isfile(expected_file))
        self.assertEqual(list(records), expected_records)

class TestCachedReadWritten(BaseTestDataProvider):

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

class TypedDataProvider(DataProvider[int]):
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
    
    def record_as_type(self, record):
        return record["test"]
    
class TestCachedReadCustomType(BaseTestDataProvider):

    def test_write_when_not_written(self):
        dp = TypedDataProvider(self.registry)
        records = dp.cached_read()
        expected_records = [1, 2]
        self.assertEqual(list(records), expected_records)


if __name__ == "__main__":
    unittest.main()

