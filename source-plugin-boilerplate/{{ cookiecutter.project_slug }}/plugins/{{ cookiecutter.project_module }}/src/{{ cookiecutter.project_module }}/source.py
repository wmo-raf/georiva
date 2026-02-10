from typing import Optional, Iterator

from georiva.sources.fetch import FileRequest
from georiva.sources.source import BaseDataSource, DataSourceType


class SourceNamePlugin(BaseDataSource):
    @property
    def name(self) -> str:
        return "Source Name"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.OBSERVATION
    
    def get_available_variables(self) -> list[dict]:
        raise NotImplementedError()
    
    def generate_requests(
            self,
            *_,
            variables: Optional[list[str]] = None,
            **kwargs
    ) -> Iterator[FileRequest]:
        raise NotImplementedError()
