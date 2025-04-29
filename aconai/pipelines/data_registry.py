from dataclasses import dataclass
import json
import os
from typing import Optional
from avro.schema import Schema, parse

@dataclass
class RegisteredFile:
    file_name: str
    is_marked_written: bool

class DataRegistry:
    """
    A class to manage the input data registry, used for caching local data.

    Note: This class is not thread-safe, and is assumed to be the sole owner
    of the data directory. If some other process writes to the directory,
    the registry may become inconsistent.
    """        
    DATA_CACHE_DIR = "DATA_CACHE_DIR"
    _SCHEMA = "schema"
    _FILES = "files"
    _IS_MARKED_WRITTEN = "is_marked_written"
    _FILE_NAME = "file_name"
    _PARAMETERS = "parameters"

    def _update_registry(self) -> None:
        """
        Updates the registry file with the current state of the registry.
        """
        json.dump(self.registry, open(self.registry_file, "w"))

    def __init__(self, data_dir: Optional[str] = None) -> None:
        """
        Initializes the DataRegistry.
        Args:
            data_dir (str, optional): The directory where the data registry will
            be stored. If not provided, it will look for the environment 
            variable DATA_CACHE_DIR, which should point to the directory. If
            this directy does not exist, it will be created.
        """
        if data_dir is None:
            data_dir = os.getenv(DataRegistry.DATA_CACHE_DIR)
            if data_dir is None:
                msg = "Environment variable DATA_CACHE_DIR is not set."
                raise ValueError(msg)
        self.data_dir = data_dir
        self.registry_file = os.path.join(data_dir, "data_registry.json")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            self.registry = {}
            self._update_registry()
        else:
            with open(self.registry_file, "r") as f:
                self.registry = json.load(f)

    def _validate_schema(self, key: str, schema: Schema) -> bool:
        """
        Validates the given schema is the same as the one stored in the
        registry for the given key. If the key is not found in the registry,
        it will be added to the registry with the given schema.
        Args:
            key (str): The key of the data input.
            schema (Schema): The schema of the data input.
        Returns:
            bool: True if the schema is valid, False otherwise.
        """
        if key not in self.registry:
            self.registry[key] = {
                DataRegistry._SCHEMA: schema.to_json(),
                DataRegistry._FILES: [],
            }
            self._update_registry()
            return True
        else:
            as_json = json.dumps(self.registry[key][DataRegistry._SCHEMA])
            stored_schema = parse(as_json)
            if stored_schema == schema:
                return True
            else:
                return False
            
    def _ensure_path_exists(self, key: str) -> str:
        """
        Ensures the path for the given key exists in the registry. If it does
        not exist, it will be created.
        Args:
            key (str): The key of the data input.
        Returns:
            str: The path for the given key.
        """
        key_folders = key.replace(".", os.sep)
        path = os.path.join(self.data_dir, key_folders)
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    def _get_next_data_file(self, path: str) -> str:
        """
        Returns the a unique file name for the given path.
        Args:
            path (str): The path to the data files.
        Returns:
            str: The latest data file in the given path.
        """
        files = os.listdir(path)
        if not files:
            latest_file = "data_0.avro"
        else:
            latest_file = max(files)
            root = os.path.splitext(latest_file)[0]
            num = int(root.split("_")[-1]) + 1
            latest_file = f"data_{num}.avro"
        return os.path.join(path, latest_file)
            
    def register(self, key: str, schema: Schema, params: dict) -> RegisteredFile:
        """
        Registers a new data input in the registry. This will validate the 
        schema matches any files already associated with the key, throwing an
        exception if it does not.

        If there's already a file associated with the key and the given
        parameters. The registry entry for that file will be returned. If the
        file has not been written yet, the caller should write it to the given
        location and call the mark_written() method to mark it as written.
        Args:
            key (str): The key of the data input.
            schema (Schema): The schema of the data input.
        Returns:
            A RegistryFile object, which includes the file name and whether
            it's marked as written or not.
        """
        if not self._validate_schema(key, schema):
            raise ValueError(f"Schema for {key} is not valid.")
        files = []
        for file in self.registry[key][DataRegistry._FILES]:
            if file[DataRegistry._PARAMETERS] == params:
                return RegisteredFile(
                    file[DataRegistry._FILE_NAME],
                    file[DataRegistry._IS_MARKED_WRITTEN]
                )
            files.append(file[DataRegistry._FILE_NAME])
        path = self._ensure_path_exists(key)
        if not files:
            latest_file = "data_0.avro"
        else:
            latest_file = max(files)
            root = os.path.splitext(latest_file)[0]
            num = int(root.split("_")[-1]) + 1
            latest_file = f"data_{num}.avro"
        file_name = os.path.join(path, latest_file)        
        self.registry[key][DataRegistry._FILES].append({
            DataRegistry._FILE_NAME: file_name,
            DataRegistry._PARAMETERS: params,
            DataRegistry._IS_MARKED_WRITTEN: False
        })
        self._update_registry()
        return RegisteredFile(file_name, False)
    
    def mark_written(self, key: str, file_name: str) -> RegisteredFile:
        """
        Marks the given file as written in the registry. This will update the
        registry to mark the file as written.
        Args:
            key (str): The key of the data input.
            file_name (str): The name of the file to mark as written.
        Returns:
            
        """
        for file in self.registry[key][DataRegistry._FILES]:
            if file[DataRegistry._FILE_NAME] == file_name:
                file[DataRegistry._IS_MARKED_WRITTEN] = True
                self._update_registry()
                return RegisteredFile(
                    file_name,
                    True,
                )
        raise ValueError(f"File {file_name} not found in registry.")
