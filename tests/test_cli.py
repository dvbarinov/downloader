# tests/test_cli.py
import pytest
import sys
from unittest.mock import patch, MagicMock
from download_files import main, download_all


#@pytest.mark.asyncio
@patch("download_files.download_all")
@patch("download_files.load_config")
@patch("download_files.setup_logging")
def test_main_with_config(mock_setup_log, mock_load_config, mock_download_all):
    mock_load_config.return_value = {
        "download": {"url_template": "http://x.com/{1..1}.csv"},
        "http": {"timeout": {"total": 10, "connect": 5}, "retries": {"enabled": False}},
        "logging": {"level": "INFO"}
    }

    # Запуск CLI с аргументом
    main(["test_config.yaml"])

    mock_load_config.assert_called_once_with("test_config.yaml")
    mock_download_all.assert_awaited_once()


@patch("download_files.load_config")
def test_main_default_config(mock_load_config):
    mock_load_config.return_value = {
        "download": {"url_template": "http://x.com/{1..1}.csv", "output_dir": "./downloads"},
        "http": {"timeout": {"total": 10, "connect": 5}},
        "logging": {}
    }

    main([])  # без аргументов → использует config.yaml

    mock_load_config.assert_called_once_with("config.yaml")