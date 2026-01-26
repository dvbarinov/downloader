# tests/test_core.py
import pytest
from download_files import expand_wildcard_url, load_config
import tempfile
import yaml
from pathlib import Path


def test_expand_wildcard_simple():
    template = "http://example.com/file_{1..3}.csv"
    expected = [
        "http://example.com/file_1.csv",
        "http://example.com/file_2.csv",
        "http://example.com/file_3.csv",
    ]
    assert expand_wildcard_url(template) == expected


def test_expand_wildcard_leading_zeros():
    template = "https://data.org/img_{001..003}.png"
    expected = [
        "https://data.org/img_001.png",
        "https://data.org/img_002.png",
        "https://data.org/img_003.png",
    ]
    assert expand_wildcard_url(template) == expected


def test_expand_wildcard_invalid_range():
    with pytest.raises(ValueError, match="Начало диапазона не может быть больше конца"):
        expand_wildcard_url("http://x.com/{5..3}.bin")


def test_expand_wildcard_no_braces():
    with pytest.raises(ValueError, match="Шаблон должен содержать"):
        expand_wildcard_url("http://x.com/file.csv")


def test_load_config():
    config_data = {
        "download": {
            "url_template": "http://test.com/data_{1..2}.json",
            "output_dir": "./out",
            "max_concurrent": 5
        },
        "http": {
            "timeout": {"total": 30, "connect": 10},
            "retries": {"enabled": True, "max_attempts": 2}
        },
        "logging": {"level": "DEBUG"}
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        f.flush()
        config = load_config(f.name)
        assert config["download"]["url_template"] == "http://test.com/data_{1..2}.json"
        assert config["http"]["retries"]["max_attempts"] == 2