"""Tests for the end-to-end pipeline entry point."""

from __future__ import annotations

import pytest

from config.databricks_runtime import (
    DEFAULT_DBFS_SAMPLE_DATA_PATH,
    detect_databricks_repo_root,
    discover_sample_data_dirs,
    prepare_config_source_for_spark,
    should_use_repo_workspace_data,
    to_spark_readable_path,
    workspace_data_path,
)
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


def test_validate_configuration_rejects_filestore_on_databricks(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3.x-scala2.12")
    config = load_config(source_base_path=DEFAULT_DBFS_SAMPLE_DATA_PATH)
    with pytest.raises(PipelineConfigurationError, match="FileStore"):
        validate_configuration(config, spark=object())


def test_prepare_config_stages_to_workspace_repo_data(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()
    for name in ("customers.csv", "products.csv", "orders.csv"):
        (staging / name).write_text("x\n", encoding="utf-8")

    config = load_config(source_base_path=DEFAULT_DBFS_SAMPLE_DATA_PATH)
    prepared = prepare_config_source_for_spark(
        spark=object(),
        config=config,
        local_csv_dir=str(staging),
        repo_root=str(repo_root),
    )

    data_dir = repo_root / "data"
    assert data_dir.is_dir()
    assert all((data_dir / name).exists() for name in ("customers.csv", "products.csv", "orders.csv"))
    assert prepared.source_base_path == workspace_data_path(str(repo_root))


def test_should_use_repo_workspace_data_for_relative_paths(tmp_path) -> None:
    repo_root = tmp_path / "databricks-medallion-pipeline"
    repo_root.mkdir()
    repo_data = repo_root / "data"
    repo_data.mkdir()

    assert should_use_repo_workspace_data("./data")
    assert should_use_repo_workspace_data(".ecommerce_sample_data")
    assert should_use_repo_workspace_data(
        "/Workspace/Users/user@domain.com/.ecommerce_sample_data",
        str(repo_root),
    )
    assert should_use_repo_workspace_data(DEFAULT_DBFS_SAMPLE_DATA_PATH)
    assert not should_use_repo_workspace_data("/Volumes/catalog/schema/volume/data")
    assert not should_use_repo_workspace_data(
        repo_data.resolve().as_uri(),
        str(repo_root),
    )


def test_to_spark_readable_path_uses_file_prefix_for_workspace() -> None:
    path = "/Workspace/Users/user@domain.com/databricks-medallion-pipeline/data/customers.csv"
    assert to_spark_readable_path(path) == (
        "file:/Workspace/Users/user@domain.com/databricks-medallion-pipeline/data/customers.csv"
    )


def test_prepare_config_stages_legacy_home_sample_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")
    repo_root = tmp_path / "databricks-medallion-pipeline"
    repo_root.mkdir()
    (repo_root / "src").mkdir()
    monkeypatch.setenv("PIPELINE_REPO_ROOT", str(repo_root))

    legacy_dir = tmp_path / ".ecommerce_sample_data"
    legacy_dir.mkdir()
    for name in ("customers.csv", "products.csv", "orders.csv"):
        (legacy_dir / name).write_text("x\n", encoding="utf-8")

    config = load_config(source_base_path=str(legacy_dir))
    prepared = prepare_config_source_for_spark(spark=object(), config=config)

    data_dir = repo_root / "data"
    assert all((data_dir / name).exists() for name in ("customers.csv", "products.csv", "orders.csv"))
    assert prepared.source_base_path == workspace_data_path(str(repo_root))


def test_discover_sample_data_dirs_includes_legacy_locations(tmp_path) -> None:
    repo_root = tmp_path / "databricks-medallion-pipeline"
    repo_root.mkdir()
    legacy_dir = tmp_path / ".ecommerce_sample_data"
    legacy_dir.mkdir()

    discovered = discover_sample_data_dirs(str(legacy_dir), str(repo_root), None)
    assert legacy_dir in discovered
    assert Path("/tmp/ecommerce_medallion_sample_data") in discovered


def test_prepare_config_rewrites_user_home_sample_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")
    repo_root = tmp_path / "databricks-medallion-pipeline"
    repo_root.mkdir()
    (repo_root / "src").mkdir()
    monkeypatch.setenv("PIPELINE_REPO_ROOT", str(repo_root))

    home_sample = tmp_path / ".ecommerce_sample_data"
    config = load_config(source_base_path=str(home_sample))
    prepared = prepare_config_source_for_spark(spark=object(), config=config)
    assert prepared.source_base_path == workspace_data_path(str(repo_root))


def test_prepare_config_rewrites_relative_path_to_repo_data(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")
    monkeypatch.setenv("PIPELINE_REPO_ROOT", str(tmp_path))
    (tmp_path / "src").mkdir()

    config = load_config(source_base_path=".ecommerce_sample_data")
    prepared = prepare_config_source_for_spark(spark=object(), config=config)
    assert prepared.source_base_path == workspace_data_path(str(tmp_path))


def test_prepare_config_rewrites_filestore_when_repo_root_detected(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")
    monkeypatch.setenv("PIPELINE_REPO_ROOT", str(tmp_path))
    (tmp_path / "src").mkdir()

    config = load_config(source_base_path=DEFAULT_DBFS_SAMPLE_DATA_PATH)
    prepared = prepare_config_source_for_spark(spark=object(), config=config)
    assert prepared.source_base_path == workspace_data_path(str(tmp_path))


def test_detect_databricks_repo_root_from_env(tmp_path, monkeypatch) -> None:
    (tmp_path / "src").mkdir()
    monkeypatch.setenv("PIPELINE_REPO_ROOT", str(tmp_path))
    assert detect_databricks_repo_root() == str(tmp_path.resolve())


def test_prepare_config_raises_when_filestore_on_databricks_without_repo(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "1.0")

    staging = tmp_path / "staging"
    staging.mkdir()
    for name in ("customers.csv", "products.csv", "orders.csv"):
        (staging / name).write_text("x\n", encoding="utf-8")

    config = load_config(source_base_path=DEFAULT_DBFS_SAMPLE_DATA_PATH)
    with pytest.raises(ValueError, match="FileStore"):
        prepare_config_source_for_spark(
            spark=object(),
            config=config,
            local_csv_dir=str(staging),
        )


def test_prepare_config_uploads_to_dbfs_when_not_on_databricks(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    staging = tmp_path / "staging"
    staging.mkdir()
    for name in ("customers.csv", "products.csv", "orders.csv"):
        (staging / name).write_text("x\n", encoding="utf-8")

    config = load_config(source_base_path=DEFAULT_DBFS_SAMPLE_DATA_PATH)
    uploads: list[tuple[str, str]] = []

    def _fake_upload(local_dir: str, dbfs_base: str, spark) -> None:
        uploads.append((local_dir, dbfs_base))

    monkeypatch.setattr(
        "config.databricks_runtime.upload_local_csvs_to_dbfs",
        _fake_upload,
    )

    prepared = prepare_config_source_for_spark(
        spark=object(),
        config=config,
        local_csv_dir=str(staging),
    )
    assert prepared.source_base_path == DEFAULT_DBFS_SAMPLE_DATA_PATH
    assert uploads == [(str(staging), DEFAULT_DBFS_SAMPLE_DATA_PATH)]
