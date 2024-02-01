import json
from typing import Any, Dict, List, Optional, Union

from aws_cdk import RemovalPolicy, SecretValue, Stack
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import aws_ssm as ssm

from stacks.constants import (
    FLD_CONTEXT_SALESFORCE_CONSUMER_KEY,
    FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET,
    FLD_CONTEXT_SF_DOMAIN,
)
from cddo.utils.constants import (
    FLD_ACCOUNT,
    FLD_DOMAIN_RELATION,
    FLD_ORPHAN_ACCOUNT,
    PS_SALESFORCE_CLIENT_ID,
    PS_SALESFORCE_CLIENT_SECRET,
    PS_SALESFORCE_DOMAIN,
    PS_SALESFORCE_EVENT_ROOT,
    PS_SALESFORCE_LAST_CHECKED,
    PS_UPDATES_FROM_SALESFORCE_BUCKET,
)


def _set_secret(
    stack: Stack,
    key_root: str,
    value: Union[str, Dict[str, str]],
    description: str,
    secret_name: Optional[str] = None,
) -> str:
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

    return secret.secret_arn


def _set_param(
    stack: Stack,
    key_root: str,
    parameter_name: str,
    string_value: Union[List[str], str],
    description: str,
) -> str:
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
    return param.parameter_arn


def create_secrets_and_params(
    stack: Stack, salesforce_context: Dict[str, Any], from_salesforce_bucket_name: str
) -> List[str]:
    arns = []
    arns.append(
        _set_secret(
            stack=stack,
            key_root=PS_SALESFORCE_EVENT_ROOT,
            value={
                PS_SALESFORCE_CLIENT_ID: salesforce_context[
                    FLD_CONTEXT_SALESFORCE_CONSUMER_KEY
                ],
                PS_SALESFORCE_CLIENT_SECRET: salesforce_context[
                    FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET
                ],
                PS_SALESFORCE_DOMAIN: salesforce_context[FLD_CONTEXT_SF_DOMAIN],
            },
            description="Salesforce client id and client_secret to called endpoint",
        )
    )

    arns.append(
        _set_param(
            stack=stack,
            key_root=PS_SALESFORCE_EVENT_ROOT,
            parameter_name=PS_SALESFORCE_LAST_CHECKED,
            string_value=json.dumps(
                {
                    FLD_ACCOUNT: "2024-01-01T00:00:00Z",
                    FLD_DOMAIN_RELATION: "2024-01-01T00:00:00Z",
                    FLD_ORPHAN_ACCOUNT: "2024-01-01T00:00:00Z",
                }
            ),
            description="Time of last check for Salesforce updates",
        )
    )
    arns.append(
        _set_param(
            stack=stack,
            key_root=PS_SALESFORCE_EVENT_ROOT,
            parameter_name=PS_UPDATES_FROM_SALESFORCE_BUCKET,
            string_value=from_salesforce_bucket_name,
            description="Store of all JSON representing updates from salesforce",
        )
    )
    return arns
