from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from django.test import SimpleTestCase

from georiva.ingestion.extractor import VariableExtractor


def _block(block_type, name):
    return SimpleNamespace(block_type=block_type, value={"source_name": name})


class GetMetadataSourceTests(SimpleTestCase):
    def _metadata_name(self, blocks):
        plugin = MagicMock()
        variable = SimpleNamespace(slug="v", sources=blocks)
        VariableExtractor(plugin).get_metadata(variable, Path("f.grib2"))
        return plugin.get_metadata_for_variable.call_args.kwargs["variable_name"]

    def test_primary_is_used_when_present(self):
        self.assertEqual(self._metadata_name([_block("primary", "2t")]), "2t")

    def test_vector_variable_falls_back_to_u_component(self):
        blocks = [_block("u_component", "10u"), _block("v_component", "10v")]
        self.assertEqual(self._metadata_name(blocks), "10u")
