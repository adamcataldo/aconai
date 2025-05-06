import json
from typing import Generic, Iterator, TypeVar, final
from aconai.pipelines.data_registry import DataRegistry
from avro.schema import parse
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from abc import ABC, abstractmethod

T = TypeVar("T")

class DataProvider(ABC, Generic[T]):
    """
    A base class for data input providers. This class is used to cache retrieved
    data input in a first-party file system, so that subsequent calls for the
    same data with the same parameters will be served from the local cache.

    Each data provider needs to implement 
    """

    def __init__(self, registry: DataRegistry) -> None:        
        self.registry = registry

    @final
    def cached_read(self) -> Iterator[T]:
        """
        Reads the data from the data provider. If the data is already stored
        in the cache, it will be read from there. Otherwise, it will be 
        retrieved by delegating to the get_records() method, and stroring the
        data in the cache.
        """
        schema = parse(json.dumps(self.get_schema()))
        registered_file = self.registry.register(
            self.registry_key(), schema, self.get_parameters()
        )
        file_name = registered_file.file_name
        if not registered_file.is_marked_written:
            writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), schema)
            for record in self.get_records():
                writer.append(record)
            writer.close()
            self.registry.mark_written(self.registry_key(), file_name)       
        reader = DataFileReader(open(file_name, "rb"), DatumReader())
        return map(lambda x: self.record_as_type(x), reader)
    
    def registry_key(self) -> str:
        """
        Returns the registry key for the data provider. This defaults to the 
        fully qualified name of the class. This can be overridden if subclasses
        get moved, so that the registry can stay consistent.
        """
        return f"{self.__module__}.{self.__class__.__name__}"
    
    def get_avro_namespace(self) -> str:
        """
        Returns the avro namespace for the data provider. This defaults to the
        fully qualified name of the containing module. This can be overridden if
        subclasses get moved, so that the registry can stay consistent.
        """
        return f"{self.__module__}"

    @abstractmethod
    def get_schema(self) -> dict:
        """
        Returns the schema of the data provider. This is used to validate
        the data input before it is cached. This should be overridden by
        subclasses to return a Python dict representation of the schema used
        for storage.
        """
        pass

    @abstractmethod
    def get_parameters(self) -> dict:
        """
        Returns the parameters of the data provider. This is used to validate
        the data input before it is cached. This should be overridden by
        subclasses to return the parameters of the data provider.
        """
        pass
    
    @abstractmethod
    def get_records(self) -> Iterator[dict]:
        """
        Returns the records of the data provider. This is used to validate the
        data input before it is cached. This should be overridden by subclasses
        to return the records of the data provider.
        """
        pass

    def record_as_type(self, record: object) -> T:
        """
        Converts a record to the type T. By default, this just returns the
        record. Subclasses can override this to have custom types returend by
        cached_read() method.
        """
        return record
