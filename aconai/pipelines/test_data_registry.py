import json
import os
import unittest
from unittest.mock import patch
from aconai.pipelines.data_registry import DataRegistry, RegisteredFile
import shutil
from avro.schema import parse

class BaseTestDataRegistry(unittest.TestCase):
    def setUp(self):
        self.data_dir = "/tmp/data_cache"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

    def tearDown(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

class TestDataRegistryInit(BaseTestDataRegistry):
    def test_raises_value_error_without_env_var(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                DataRegistry()

    def test_registry_file_exists_after_creation(self):
        DataRegistry(self.data_dir)
        path = os.path.join(self.data_dir, "data_registry.json")
        self.assertTrue(os.path.exists(path))

class TestDataRegistryValidateSchema(BaseTestDataRegistry):

    def test_validate_schema(self):
        registry = DataRegistry(self.data_dir)
        schema_1 = parse(json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }))
        schema_2 = parse(json.dumps({
            "type": "record",
            "name": "TestRecord2",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
                {"name": "field3", "type": "float"}
            ]
        }))
        key = "test.key"
        self.assertTrue(registry._validate_schema(key, schema_1))
        self.assertTrue(registry._validate_schema(key, schema_1))
        self.assertFalse(registry._validate_schema(key, schema_2))

    def test_validate_schema_persistence(self):
        registry_1 = DataRegistry(self.data_dir)
        schema_1 = parse(json.dumps({
            "type": "record",
            "name": "TestRecord2",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
                {"name": "field3", "type": "float"}
            ]
        }))
        key = "test.key.persistence"
        self.assertTrue(registry_1._validate_schema(key, schema_1))
        
        registry_2 = DataRegistry(self.data_dir)
        schema_2 = parse(json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }))
        self.assertFalse(registry_2._validate_schema(key, schema_2))
        self.assertTrue(registry_2._validate_schema(key, schema_1))

class TestDataRegistryEnsurePathExists(BaseTestDataRegistry):

    def test_ensure_path_exists_creates_path(self):
        registry = DataRegistry(self.data_dir)
        key = "test.key"
        path = registry._ensure_path_exists(key)
        expected_path = "/tmp/data_cache/test/key"
        self.assertTrue(os.path.exists(path))
        self.assertEqual(path, expected_path)

class TestDataRegistryRegister(BaseTestDataRegistry):

    def test_register_with_new_key(self):
        registry = DataRegistry(self.data_dir)
        key = "test.key.t1"
        schema = parse(json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }))
        params = {"param1": "value1", "param2": "value2"}
        registered_file = registry.register(key, schema, params)
        expected_registered_file = RegisteredFile(
            "/tmp/data_cache/test/key/t1/data_0.avro",
            False
        )
        self.assertEqual(registered_file, expected_registered_file)

    def test_register_with_new_params(self):
        registry = DataRegistry(self.data_dir)
        key = "test.key.t2"
        schema = parse(json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }))
        params_1 = {"param1": "value1", "param2": "value2"}
        params_2 = {"param1": "value2", "param2": "value1"}
        registered_file_1 = registry.register(key, schema, params_1)
        registered_file_2 = registry.register(key, schema, params_2)
        expected_registered_file_1 = RegisteredFile(
            "/tmp/data_cache/test/key/t2/data_0.avro",
            False
        )
        expected_registered_file_2 = RegisteredFile(
            "/tmp/data_cache/test/key/t2/data_1.avro",
            False
        )
        self.assertEqual(registered_file_1, expected_registered_file_1)
        self.assertEqual(registered_file_2, expected_registered_file_2)


class TestDataRegistryMarkWritten(BaseTestDataRegistry):

    def test_mark_written(self):
        registry = DataRegistry(self.data_dir)
        key = "test.key.t1"
        schema = parse(json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }))
        params = {"param1": "value1", "param2": "value2"}
        registered_file_1 = registry.register(key, schema, params)
        expected_registered_file_1 = RegisteredFile(
            "/tmp/data_cache/test/key/t1/data_0.avro",
            False
        )
        self.assertEqual(registered_file_1, expected_registered_file_1)
        registered_file_2 = registry.mark_written(key, registered_file_1.file_name)
        expected_registered_file_2 = RegisteredFile(
            "/tmp/data_cache/test/key/t1/data_0.avro",
            True
        )
        self.assertEqual(registered_file_2, expected_registered_file_2)


if __name__ == '__main__':
    unittest.main()