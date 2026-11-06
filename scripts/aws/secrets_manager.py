import json
import boto3
from botocore.exceptions import ClientError
from scripts.models.config_models import AwsConfig

SECRET_SERVICE = 'secretsmanager'


def get_secret(aws_info: AwsConfig | dict) -> str:
    region = aws_info.region if isinstance(aws_info, AwsConfig) else aws_info["region"]
    secret_name = aws_info.api_secret if isinstance(aws_info, AwsConfig) else aws_info["api_secret"]

    session = boto3.session.Session()
    client = session.client(
        service_name=SECRET_SERVICE,
        region_name=region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    # If the secret is stored as a JSON key-value object in Secrets Manager, extract the token string
    try:
        parsed = json.loads(secret)
        if isinstance(parsed, dict):
            if secret_name in parsed:
                return str(parsed[secret_name])
            if len(parsed) == 1:
                return str(next(iter(parsed.values())))
    except (json.JSONDecodeError, TypeError):
        pass

    return secret
