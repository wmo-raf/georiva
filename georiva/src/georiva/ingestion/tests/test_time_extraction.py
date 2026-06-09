from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from georiva.ingestion.time_extraction import extract_times


class GRPrefixExtractionTests(SimpleTestCase):

    def test_gr_prefix_yields_reference_time(self):
        result = extract_times("GR--20250115T0600--20250115.grib2", "YYYYMMDD")
        self.assertEqual(result["reference_time"], datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc))

    def test_no_gr_prefix_yields_no_reference_time(self):
        result = extract_times("20250115.grib2", "YYYYMMDD")
        self.assertNotIn("reference_time", result)

    def test_gr_prefix_and_valid_time_both_extracted(self):
        result = extract_times("GR--20250115T0600--20250116.grib2", "YYYYMMDD")
        self.assertEqual(result["reference_time"], datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc))
        self.assertEqual(result["valid_time"], datetime(2025, 1, 16, 0, 0, tzinfo=timezone.utc))


class ValidTimeStemParsingTests(SimpleTestCase):

    def test_YYYYMMDD(self):
        result = extract_times("20250115.grib2", "YYYYMMDD")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, tzinfo=timezone.utc))

    def test_DDMMYYYY(self):
        result = extract_times("15012025.grib2", "DDMMYYYY")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, tzinfo=timezone.utc))

    def test_YYYYMMDDHH(self):
        result = extract_times("2025011506.grib2", "YYYYMMDDHH")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, 6, tzinfo=timezone.utc))

    def test_YYYYMMDDHHMM(self):
        result = extract_times("202501150630.grib2", "YYYYMMDDHHMM")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, 6, 30, tzinfo=timezone.utc))

    def test_DDMMYY(self):
        result = extract_times("150125.grib2", "DDMMYY")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, tzinfo=timezone.utc))

    def test_YYMMDD(self):
        result = extract_times("250115.grib2", "YYMMDD")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, tzinfo=timezone.utc))

    def test_path_with_directories_uses_filename_stem(self):
        result = extract_times("catalog/collection/20250115.grib2", "YYYYMMDD")
        self.assertEqual(result["valid_time"], datetime(2025, 1, 15, tzinfo=timezone.utc))


class EmptyResultTests(SimpleTestCase):

    def test_unrecognisable_stem_returns_empty_dict(self):
        result = extract_times("chirps-v2.0.grib2", "YYYYMMDD")
        self.assertEqual(result, {})

    def test_unknown_format_choice_returns_empty_dict(self):
        result = extract_times("20250115.grib2", "UNKNOWN")
        self.assertEqual(result, {})

    def test_no_exception_on_garbage_filename(self):
        result = extract_times("not-a-date-at-all.tif", "YYYYMMDD")
        self.assertIsInstance(result, dict)


class ContentFallbackTests(SimpleTestCase):

    def test_valid_time_read_from_grib_content_when_stem_unrecognised(self):
        expected_ts = datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc)
        mock_plugin = MagicMock()
        mock_plugin.list_variables.return_value = [{"name": "2t"}]
        mock_plugin.get_timestamps.return_value = [expected_ts]

        with patch("georiva.ingestion.time_extraction.format_registry") as mock_registry:
            mock_registry.get_for_file.return_value = mock_plugin
            result = extract_times("unrecognised.grib2", "YYYYMMDD", file_obj=BytesIO(b"fake"))

        self.assertEqual(result["valid_time"], expected_ts)

    def test_non_grib_netcdf_file_obj_ignored_for_content_fallback(self):
        with patch("georiva.ingestion.time_extraction.format_registry") as mock_registry:
            result = extract_times("file.tif", "YYYYMMDD", file_obj=BytesIO(b"fake"))

        mock_registry.get_for_file.assert_not_called()
        self.assertEqual(result, {})

    def test_content_fallback_not_called_when_both_fields_resolved_from_filename(self):
        with patch("georiva.ingestion.time_extraction.format_registry") as mock_registry:
            result = extract_times("GR--20250115T0600--20250116.grib2", "YYYYMMDD", file_obj=BytesIO(b"fake"))

        mock_registry.get_for_file.assert_not_called()
        self.assertIn("reference_time", result)
        self.assertIn("valid_time", result)

    def test_no_exception_when_plugin_raises(self):
        with patch("georiva.ingestion.time_extraction.format_registry") as mock_registry:
            mock_registry.get_plugin_for.side_effect = RuntimeError("plugin error")
            result = extract_times("unrecognised.grib2", "YYYYMMDD", file_obj=BytesIO(b"fake"))

        self.assertIsInstance(result, dict)
