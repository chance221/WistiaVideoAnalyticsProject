import boto3
from botocore.exceptions import ClientError
from scripts.models.config_models import AWSConfig

SECRET_SERVICE = 'secretsmanager'

def get_secret(aws_info: dict):
    

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name= SECRET_SERVICE,
        region_name=aws_info["region"]
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId= aws_info["secret_name"]
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    return secret
