"""Tests for the end-to-end pipeline entry point."""

from __future__ import annotations

import pytest

from config.pipeline_config import load_config
from run_pipeline import (
    PipelineConfigurationError,
    is_remote_path,
    resolve_sample_data_output_dir,
    validate_configuration,
)


def test_validate_configuration_accepts_defaults() -> None:
    config = load_config(schema_name="ecommerce", source_base_path="dbfs:/FileStore/ecommerce/data")
    validate_configuration(config)


def test_validate_configuration_rejects_empty_schema() -> None:
    config = load_config(schema_name="  ", source_base_path="dbfs:/data")
    with pytest.raises(PipelineConfigurationError, match="schema_name"):
        validate_configuration(config)


def test_validate_configuration_rejects_invalid_write_mode() -> None:
    config = load_config(bronze_write_mode="replace")
    with pytest.raises(PipelineConfigurationError, match="bronze_write_mode"):
        validate_configuration(config)


def test_is_remote_path_detects_dbfs() -> None:
    assert is_remote_path("dbfs:/FileStore/ecommerce/data")
    assert is_remote_path("/dbfs/FileStore/data")
    assert not is_remote_path("./data")


def test_resolve_sample_data_output_dir_local_source() -> None:
    config = load_config(source_base_path="./data")
    resolved = resolve_sample_data_output_dir(config, cli_output_dir=None)
    assert resolved == resolve_sample_data_output_dir(config, cli_output_dir=None)


def test_resolve_sample_data_output_dir_requires_override_for_remote() -> None:
    config = load_config(source_base_path="dbfs:/FileStore/ecommerce/data")
    with pytest.raises(PipelineConfigurationError, match="sample-data-output-dir"):
        resolve_sample_data_output_dir(config, cli_output_dir=None)


def test_validate_configuration_rejects_file_tmp_on_databricks(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3.x-scala2.12")
    config = load_config(source_base_path="file:/tmp/ecommerce_medallion_sample_data")
    with pytest.raises(PipelineConfigurationError, match="not readable by Spark on Databricks"):
        validate_configuration(config)
