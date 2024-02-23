import json
from typing import Any, Dict, List, Optional, Tuple, Union

from aws_cdk import RemovalPolicy, SecretValue, Stack
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import aws_ssm as ssm
from cddo.utils.constants import (FLD_DOMAIN_RELATION, FLD_ORGANISATION,
                                  FLD_ORPHAN_ORGANISATION,
                                  PS_SALESFORCE_CLIENT_ID,
                                  PS_SALESFORCE_CLIENT_SECRET,
                                  PS_SALESFORCE_DOMAIN,
                                  PS_SALESFORCE_EVENT_ROOT,
                                  PS_SALESFORCE_LAST_CHECKED)

from stacks.constants import (FLD_CONTEXT_SALESFORCE_CONSUMER_KEY,
                              FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET,
                              FLD_CONTEXT_SF_DOMAIN)


def set_secret(
    stack: Stack,
    key_root: str,
    value: Union[str, Dict[str, str]],
    description: str,
    secret_name: Optional[str] = None,
) -> sm.Secret:
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

    return secret


def set_param(
    stack: Stack,
    key_root: str,
    parameter_name: str,
    string_value: Union[List[str], str],
    description: str,
) -> ssm.IParameter:
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
    return param


def create_secrets_and_params(
    stack: Stack, context: Dict[str, Any]
) -> Tuple[sm.Secret, ssm.IParameter]:
    salesforce_secret = set_secret(
        stack=stack,
        key_root=PS_SALESFORCE_EVENT_ROOT,
        value={
            PS_SALESFORCE_CLIENT_ID: context[FLD_CONTEXT_SALESFORCE_CONSUMER_KEY],
            PS_SALESFORCE_CLIENT_SECRET: context[
                FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET
            ],
            PS_SALESFORCE_DOMAIN: context[FLD_CONTEXT_SF_DOMAIN],
        },
        description="Salesforce client id and client_secret to called endpoint",
    )

    last_checked_param = set_param(
        stack=stack,
        key_root=PS_SALESFORCE_EVENT_ROOT,
        parameter_name=PS_SALESFORCE_LAST_CHECKED,
        string_value=json.dumps(
            {
                FLD_ORGANISATION: "2024-01-01T00:00:00Z",
                FLD_DOMAIN_RELATION: "2024-01-01T00:00:00Z",
                FLD_ORPHAN_ORGANISATION: "2024-01-01T00:00:00Z",
            }
        ),
        description="Time of last check for Salesforce updates",
    )
    return salesforce_secret, last_checked_param
