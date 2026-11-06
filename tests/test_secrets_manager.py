import json
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import pytest
from scripts.aws.secrets_manager import get_secret
from scripts.models.config_models import AwsConfig


@patch("scripts.aws.secrets_manager.boto3.session.Session")
def test_get_secret_plaintext_with_aws_config(mock_session_cls: MagicMock):
  mock_client = MagicMock()
  mock_client.get_secret_value.return_value = {"SecretString": "raw-token-123"}
  mock_session_cls.return_value.client.return_value = mock_client

  aws_cfg = AwsConfig(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      api_secret="westia_api_key",
  )
  token = get_secret(aws_cfg)

  assert token == "raw-token-123"
  mock_client.get_secret_value.assert_called_once_with(SecretId="westia_api_key")


@patch("scripts.aws.secrets_manager.boto3.session.Session")
def test_get_secret_json_key_value_unwrap(mock_session_cls: MagicMock):
  mock_client = MagicMock()
  mock_client.get_secret_value.return_value = {
      "SecretString": json.dumps({"westia_api_key": "unwrapped-token-456"})
  }
  mock_session_cls.return_value.client.return_value = mock_client

  token = get_secret({"region": "us-east-1", "api_secret": "westia_api_key"})
  assert token == "unwrapped-token-456"


@patch("scripts.aws.secrets_manager.boto3.session.Session")
def test_get_secret_raises_client_error(mock_session_cls: MagicMock):
  mock_client = MagicMock()
  mock_client.get_secret_value.side_effect = ClientError(
      {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
      "GetSecretValue",
  )
  mock_session_cls.return_value.client.return_value = mock_client

  with pytest.raises(ClientError):
    get_secret({"region": "us-east-1", "api_secret": "missing_key"})
