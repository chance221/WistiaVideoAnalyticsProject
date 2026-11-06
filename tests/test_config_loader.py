from pathlib import Path
import pytest
from scripts.models.config_loader import (
    get_config,
    load_environment_config,
    load_media_config,
)


def test_load_actual_repo_configs():
  media_cfg = load_media_config("media")
  assert media_cfg.api_base == "https://api.wistia.com"
  assert "9k4tbcdfg0" in media_cfg.media_ids
  assert media_cfg.results_per_page == 50
  assert set(media_cfg.endpoints.keys()) == {
      "media_views",
      "media_engagement",
      "media_events",
      "media_metadata",
  }

  dev_cfg = load_environment_config("dev")
  assert dev_cfg.env == "dev"
  assert dev_cfg.aws.bucket_name == "wistia-pipeline-project"
  assert dev_cfg.aws.region == "us-east-1"

  prod_cfg = load_environment_config("prod")
  assert prod_cfg.env == "prod"
  assert prod_cfg.aws.bucket_name == "wistia-pipeline-project"


def test_get_config_file_not_found(tmp_path: Path):
  with pytest.raises(FileNotFoundError, match="no config for config 'missing'"):
    get_config("missing", config_dir=tmp_path)


def test_get_config_rejects_non_mapping_yaml(tmp_path: Path):
  bad_yaml = tmp_path / "bad.yaml"
  bad_yaml.write_text("- item1\n- item2\n", encoding="utf-8")

  with pytest.raises(ValueError, match="expected a YAML mapping"):
    get_config("bad", config_dir=tmp_path)


def test_load_media_config_missing_sections(tmp_path: Path):
  incomplete = tmp_path / "media.yaml"
  incomplete.write_text("api_base: https://api.wistia.com\n", encoding="utf-8")

  with pytest.raises(ValueError, match="missing section"):
    load_media_config("media", config_dir=tmp_path)


def test_load_environment_config_invalid_aws_schema(tmp_path: Path):
  bad_env = tmp_path / "dev.yaml"
  bad_env.write_text(
      "env: dev\naws:\n  bucket_name: my-bucket\n",
      encoding="utf-8",
  )

  with pytest.raises(ValueError, match="aws config for 'dev' has the following issue"):
    load_environment_config("dev", config_dir=tmp_path)
