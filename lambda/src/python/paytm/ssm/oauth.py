from typing import Optional

import boto3


def get_oauth_token(aws_region: str, key_path: str) -> Optional[str]:
    ssm_client = boto3.client('ssm', region_name=aws_region)
    response = ssm_client.get_parameters_by_path(Path=key_path, Recursive=True, WithDecryption=True)

    for param in response['Parameters']:
        if param['Name'] == key_path + "/oauth-token":
            return param['Value']

    raise ValueError('Successful response, but no oauth token returned.')
