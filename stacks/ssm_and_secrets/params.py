from typing import List, Union, Dict, Optional

from aws_cdk import RemovalPolicy, Stack, SecretValue
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_secretsmanager as sm

from stacks.constants import (
    PS_SF_CLIENT_ID,
    PS_SF_CLIENT_SECRET,
    PS_SF_EVENT_ROOT,
    PS_SF_SALESFORCE_LAST_CHECKED,
)


def get_ssm_param(stack: Stack, key_root: str, parameter_name: str) -> str:
    return ssm.StringParameter.value_for_typed_string_parameter_v2(
        stack, parameter_name=f"/{key_root}/{parameter_name}"
    )


def _set_secret(
    stack: Stack,
    key_root: str,
    value: Union[str, Dict[str, str]],
    description: str,
    secret_name: Optional[str] = None,
):
    if type(value) is str:
        secret_full_name = f"/{key_root}/{secret_name}"

        secret = sm.Secret(
            stack,
            id=secret_full_name,
            description=description,
            secret_name=secret_full_name,
            secret_string_value=SecretValue.unsafe_plain_text(value),
        )
    else:
        secret = sm.Secret(
            stack,
            id=key_root,
            description=description,
            secret_name=key_root,
            secret_object_value={
                k: SecretValue.unsafe_plain_text(v) for k, v in value.items()
            },
        )

    secret.apply_removal_policy(
        RemovalPolicy.DESTROY,
    )


def _set_param(
    stack: Stack,
    key_root: str,
    parameter_name: str,
    string_value: Union[List[str], str],
    description: str,
):
    parameter_full_name = f"/{key_root}/{parameter_name}"
    if type(string_value) is list:
        param = ssm.StringListParameter(
            stack,
            parameter_name,
            string_list_value=string_value,
            description=description,
            parameter_name=parameter_full_name,
            tier=ssm.ParameterTier.INTELLIGENT_TIERING,
        )
    else:
        param = ssm.StringParameter(
            stack,
            parameter_name,
            string_value=string_value,
            data_type=ssm.ParameterDataType.TEXT,
            description=description,
            parameter_name=parameter_full_name,
            tier=ssm.ParameterTier.INTELLIGENT_TIERING,
        )
    param.apply_removal_policy(
        RemovalPolicy.DESTROY,
    )


def create_secrets_and_params(stack: Stack, client_id: str, client_secret: str) -> None:
    _set_secret(
        stack=stack,
        key_root=PS_SF_EVENT_ROOT,
        value={PS_SF_CLIENT_ID: client_id, PS_SF_CLIENT_SECRET: client_secret},
        description="Salesforce client id and client_secret to called endpoint",
    )
    _set_param(
        stack=stack,
        key_root=PS_SF_EVENT_ROOT,
        parameter_name=PS_SF_SALESFORCE_LAST_CHECKED,
        string_value="2023-01-01T00:00:00Z",
        description="Time of last check for Salesforce updates",
    )
