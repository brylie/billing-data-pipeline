from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipeline.s3_utils import S3HiveResource


@pytest.fixture
def s3_hive_resource():
    """Fixture for creating an S3HiveResource instance."""
    return S3HiveResource()


class TestS3HiveResource:
    """Test suite for the S3HiveResource class."""

    def test_parse_url_with_http(self, s3_hive_resource):
        """Test URL parsing with HTTP protocol."""
        protocol, domain, path = s3_hive_resource._parse_url(
            "http://example.com/path/to/resource"
        )
        assert protocol == "http"
        assert domain == "example.com"
        assert path == "path/to/resource"

    def test_parse_url_with_https(self, s3_hive_resource):
        """Test URL parsing with HTTPS protocol."""
        protocol, domain, path = s3_hive_resource._parse_url(
            "https://example.com/path/to/resource"
        )
        assert protocol == "https"
        assert domain == "example.com"
        assert path == "path/to/resource"

    def test_parse_url_with_s3(self, s3_hive_resource):
        """Test URL parsing with S3 protocol."""
        protocol, domain, path = s3_hive_resource._parse_url(
            "s3://my-bucket/path/to/resource"
        )
        assert protocol == "s3"
        assert domain == "my-bucket"
        assert path == "path/to/resource"

    def test_parse_url_without_protocol(self, s3_hive_resource):
        """Test URL parsing without protocol (should default to https)."""
        protocol, domain, path = s3_hive_resource._parse_url(
            "example.com/path/to/resource"
        )
        assert protocol == "https"
        assert domain == "example.com"
        assert path == "path/to/resource"

    def test_parse_url_empty_string(self, s3_hive_resource):
        """Test URL parsing with empty string."""
        protocol, domain, path = s3_hive_resource._parse_url("")
        assert protocol == "https"
        assert domain == ""
        assert path == ""

    def test_parse_url_domain_only(self, s3_hive_resource):
        """Test URL parsing with domain only."""
        protocol, domain, path = s3_hive_resource._parse_url("example.com")
        assert protocol == "https"
        assert domain == "example.com"
        assert path == ""

    def test_parse_hive_path(self, s3_hive_resource):
        """Test parsing of Hive-partitioned paths."""
        path = "year=2025/month=04/day=23/billing.csv"
        result = s3_hive_resource.parse_hive_path(path)
        assert result == {"year": "2025", "month": "04", "day": "23"}

    def test_parse_hive_path_empty(self, s3_hive_resource):
        """Test parsing empty Hive path."""
        path = ""
        result = s3_hive_resource.parse_hive_path(path)
        assert result == {}

    def test_parse_hive_path_incomplete(self, s3_hive_resource):
        """Test parsing incomplete Hive path."""
        path = "year=2025/billing.csv"
        result = s3_hive_resource.parse_hive_path(path)
        assert result == {"year": "2025"}

    def test_parse_hive_path_with_non_hive_parts(self, s3_hive_resource):
        """Test parsing Hive path with non-Hive parts."""
        path = "bucket/year=2025/folder/month=04/day=23/billing.csv"
        result = s3_hive_resource.parse_hive_path(path)
        assert result == {"year": "2025", "month": "04", "day": "23"}

    @patch("pipeline.s3_utils.get_env")
    @patch("pipeline.s3_utils.get_dagster_logger")
    def test_generate_partition_paths(
        self, mock_logger, mock_get_env, s3_hive_resource
    ):
        """Test generation of partition paths based on date range."""
        mock_get_env.return_value = "https://example.com/data"
        mock_logger.return_value = MagicMock()

        from_date = datetime(2025, 4, 23)
        to_date = datetime(2025, 4, 25)

        partitions = s3_hive_resource.generate_partition_paths(
            "https://example.com/data", from_date, to_date
        )

        assert len(partitions) == 3

        # Verify first partition
        assert partitions[0]["year"] == "2025"
        assert partitions[0]["month"] == "04"
        assert partitions[0]["day"] == "23"
        assert partitions[0]["path"] == "example.com/data/year=2025/month=04/day=23"
        assert (
            "example.com/data/year=2025/month=04/day=23/billing.csv"
            in partitions[0]["files"]
        )

        # Verify last partition
        assert partitions[2]["year"] == "2025"
        assert partitions[2]["month"] == "04"
        assert partitions[2]["day"] == "25"

    @patch("pipeline.s3_utils.get_env")
    @patch("pipeline.s3_utils.get_dagster_logger")
    def test_generate_partition_paths_default_dates(
        self, mock_logger, mock_get_env, s3_hive_resource
    ):
        """Test generation of partition paths with default dates."""
        mock_get_env.return_value = "https://example.com/data"
        mock_logger.return_value = MagicMock()

        # When not providing dates, it should use yesterday and today
        partitions = s3_hive_resource.generate_partition_paths(
            "https://example.com/data"
        )

        assert len(partitions) == 2

        # Since we're using dynamic dates, just check the structure
        assert "year" in partitions[0]
        assert "month" in partitions[0]
        assert "day" in partitions[0]
        assert "path" in partitions[0]
        assert "files" in partitions[0]

    @patch("pipeline.s3_utils.s3fs.S3FileSystem")
    def test_get_s3fs(self, mock_s3fs, s3_hive_resource):
        """Test getting an S3 filesystem client."""
        mock_s3fs_instance = MagicMock()
        mock_s3fs.return_value = mock_s3fs_instance

        fs = s3_hive_resource.get_s3fs()

        mock_s3fs.assert_called_once_with(anon=True)
        assert fs == mock_s3fs_instance

    @patch("pipeline.s3_utils.get_dagster_logger")
    @patch("pipeline.s3_utils.get_env")
    @patch("pipeline.s3_utils.S3HiveResource._parse_url")
    @patch("pipeline.s3_utils.S3HiveResource.get_s3fs")
    @patch("requests.get")
    def test_download_partition(
        self,
        mock_requests_get,
        mock_get_s3fs,
        mock_parse_url,
        mock_get_env,
        mock_logger,
        s3_hive_resource,
        tmp_path,
    ):
        """Test downloading files from a partition with fallback to HTTP."""
        # Set up mocks
        mock_logger.return_value = MagicMock()
        mock_get_env.return_value = "https://example.com/data"
        mock_parse_url.return_value = ("https", "example.com", "data")

        mock_fs = MagicMock()
        # Make s3fs download fail to test HTTP fallback
        mock_fs.get.side_effect = Exception("S3 download failed")
        mock_get_s3fs.return_value = mock_fs

        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"test data"]
        mock_requests_get.return_value = mock_response

        # Create a temporary directory for downloads
        local_dir = str(tmp_path)

        # Create a partition with a test file
        partition = {
            "year": "2025",
            "month": "04",
            "day": "23",
            "path": "example.com/data/year=2025/month=04/day=23",
            "files": ["example.com/data/year=2025/month=04/day=23/billing.csv"],
        }

        # Test the download method
        downloaded_files = s3_hive_resource.download_partition(partition, local_dir)

        # Verify S3FS was tried first
        mock_fs.get.assert_called_once()

        # Verify HTTP fallback was used
        mock_requests_get.assert_called_once()

        # Verify file was downloaded
        assert len(downloaded_files) == 1
        assert downloaded_files[0].endswith("billing-2025-04-23-billing.csv")
