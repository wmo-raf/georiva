from datetime import UTC, datetime
from unittest.mock import MagicMock

from georiva.sources.source import BaseDataSource


class _Source(BaseDataSource):
    type = "t"
    label = "T"
    name = "T"
    source_type = None

    def __init__(self):
        super().__init__({}, fetch_strategy=MagicMock())
        self.seen = {}

    def generate_requests(self, start_time, end_time, variables=None, **kwargs):
        self.seen = kwargs
        return iter([])

    def get_time_window(self, *, collection=None):
        return datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)


def test_generate_requests_for_collection_passes_collection():
    source, collection = _Source(), MagicMock()
    collection.source_variables_list.return_value = ["t2m"]

    list(source.generate_requests_for_collection(collection))

    assert source.seen["collection"] is collection
