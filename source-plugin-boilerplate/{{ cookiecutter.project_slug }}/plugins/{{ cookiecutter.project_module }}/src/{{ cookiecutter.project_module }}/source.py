"""
Data source for {{ cookiecutter.project_name }}.

A `BaseDataSource` knows *what* files to fetch and *how* to describe each as a
`FileRequest`. The actual download is delegated to a fetch strategy (here the
built-in `HTTPFetchStrategy`); GeoRiva's Loader drives fetch -> store -> ingest.

Required surface:
  * class attributes `type` (unique machine key) and `label` (human-readable)
  * `__init__(self, config, fetch_strategy=...)` — pass a fetch strategy up
  * `name`, `source_type` properties
  * `generate_requests(start_time, end_time, variables=None)` — the core method

`BaseDataSource.__init__` raises if `type`, `label`, or `fetch_strategy` are
unset, so all three must be provided.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple

from georiva.sources.fetch import FileRequest, HTTPFetchStrategy
from georiva.sources.source import BaseDataSource, DataSourceType


class {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}DataSource(BaseDataSource):
    type = "{{ cookiecutter.project_module }}"   # unique across all plugins; used in logging
    label = "{{ cookiecutter.project_name }}"

    def __init__(self, config: dict, fetch_strategy=HTTPFetchStrategy):
        super().__init__(config, fetch_strategy)
        # `config` is the merge of DataFeed.get_loader_config() (feed-wide) and
        # the per-collection DataFeedCollectionLink.config. Unpack it here.
        self.requested_variables = config.get("variables", [])

    @property
    def name(self) -> str:
        return self.label

    @property
    def source_type(self) -> DataSourceType:
        # One of: FORECAST, REANALYSIS, SATELLITE, OBSERVATION, DERIVED.
        return DataSourceType.OBSERVATION

    def generate_requests(
            self,
            start_time: datetime,
            end_time: datetime,
            variables: Optional[list[str]] = None,
            **kwargs,
    ) -> Iterator[FileRequest]:
        """Yield one FileRequest per file to download in the [start, end] window."""
        variables = variables or self.requested_variables

        # TODO: build the real list of files for the window. Example:
        #
        # url = f"https://example.com/data/{start_time:%Y%m%d}.tif"
        # yield FileRequest(
        #     identifier=f"example-{start_time:%Y%m%d}",
        #     # embed an ISO timestamp the format plugin can parse back out:
        #     filename=f"example_{start_time:%Y-%m-%dT%H:%M:%S}.tif",
        #     valid_time=start_time,
        #     reference_time=None,          # set the model run time for forecasts
        #     params={"url": url, "variables": variables},
        #     expected_format="geotiff",    # geotiff | grib | netcdf
        #     variables=variables,
        # )
        raise NotImplementedError("Implement generate_requests() for your source")

    # Optional: discover the newest available timestep (for scheduled feeds).
    # def get_latest_available(self) -> Optional[datetime]:
    #     return None

    # Optional: convert/rename the downloaded file before it is stored in MinIO.
    # Return (new_path, new_filename) — or (local_path, None) to leave it as-is.
    # def post_process_fetched_file(self, request, local_path: Path) -> Tuple[Path, Optional[str]]:
    #     return local_path, None
