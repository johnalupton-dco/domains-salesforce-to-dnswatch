from typing import Any, Dict, List, Optional, Tuple

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import aws_ssm as ssm
from cddo.utils import constants as cnst
from cddo.utils import lambdas
from cddo.utils.constants import LL_REQUESTS

from stacks.constants import LL_CDDO_UTILS, FLD_CONTEXT_RDSSECRETNAME
from .vpc import get_rds_vpc


ENV_UPDATE_FROM_SALESFORCE_BUCKET = "CDDO_UPDATE_FROM_SALESFORCE_BUCKET"

layers = {}


def _get_lambda_layers(
    stack: Stack, layer_names: List[str], profile: str
) -> Dict[str, lambda_.LayerVersion]:
    bucket_name = f"{cnst.LL_LAYER_BUCKET_ROOT}{profile}"
    bucket = s3.Bucket.from_bucket_name(stack, id=bucket_name, bucket_name=bucket_name)
    for layer_name in layer_names:
        code = lambda_.Code.from_bucket(
            bucket=bucket,
            key=f"{layer_name}.zip",
        )
        layers[layer_name] = lambda_.LayerVersion(
            stack,
            id=layer_name,
            layer_version_name=layer_name.replace(".", "-"),
            code=code,
            compatible_architectures=None,
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            removal_policy=RemovalPolicy.DESTROY,
        )

    return layers


def _create_lambda_task(
    stack: cdk.Stack,
    task_name: str,
    description: str,
    policy_statements: Optional[list[iam.PolicyStatement]] = None,
    security_groups: Optional[List[ec2.SecurityGroup]] = None,
    vpc: Optional[ec2.Vpc] = None,
    vpc_subnets: Optional[Any] = None,
    environment: Dict[str, str] = None,
    memory_size: Optional[int] = 512,
    timeout: int = 900,
    lambda_function: Optional[lambda_.Function] = None,
) -> Tuple[tasks.LambdaInvoke, lambda_.Function]:
    if not lambda_function:
        lambda_function = lambdas.create_lambda(
            stack=stack,
            name=task_name,
            description=description,
            timeout=timeout,
            source_folder="./lambdas",
            create_log_group=True,
            lambda_layers=[l for k, l in layers.items()],
            environment=environment,
            security_groups=security_groups,
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            memory_size=memory_size,
        )
    if policy_statements:
        for p in policy_statements:
            lambda_function.add_to_role_policy(p)

    return (
        tasks.LambdaInvoke(
            payload_response_only=True,
            scope=stack,
            id=task_name,
            state_name=task_name,
            retry_on_service_exceptions=False,
            lambda_function=lambda_function,
        ),
        lambda_function,
    )


def create_queue_consume_state_machine(
    stack: cdk.Stack,
    tables: List[ddb.TableV2],
    profile: str,
    context: Dict[str, str],
    from_salesforce_bucket: s3.Bucket,
    salesforce_secret: sm.Secret,
    last_checked_param: ssm.IParameter,
) -> sfn.StateMachine:
    LL_PSYCOPG = "python_psycopg_layer"
    LL_SQLALCHEMY = "python_sqlalchemy_layer"
    LL_PANDAS = "python_pandas_layer"

    _get_lambda_layers(
        stack,
        [LL_CDDO_UTILS, LL_REQUESTS, LL_PSYCOPG, LL_SQLALCHEMY, LL_PANDAS],
        profile=profile,
    )

    state_machine_name = "SendSalesforceUpdatesToDNSWatch"

    task_start_sf_update, fn = _create_lambda_task(
        stack=stack,
        task_name="GetSalesforceChanges",
        description="Query Salesforce with REST API to find updated data",
        environment={
            ENV_UPDATE_FROM_SALESFORCE_BUCKET: from_salesforce_bucket.bucket_name
        },
        memory_size=2048,
    )
    from_salesforce_bucket.grant_put(fn)
    salesforce_secret.grant_read(fn)
    last_checked_param.grant_read(fn)
    last_checked_param.grant_write(fn)

    for t in tables:
        t.grant_write_data(fn)

    rds_secret = sm.Secret.from_secret_name_v2(
        scope=stack,
        id="RDSSecret",
        secret_name=context[FLD_CONTEXT_RDSSECRETNAME],
    )

    vpc, security_group, vpc_subnets, environment = get_rds_vpc(
        stack=stack, context=context
    )

    environment[ENV_UPDATE_FROM_SALESFORCE_BUCKET] = from_salesforce_bucket.bucket_name

    task_complete_sf_update, fn = _create_lambda_task(
        stack=stack,
        task_name="FinaliseSalesforceUpdate",
        description="Finalise Salesforce update",
        security_groups=[security_group],
        vpc=vpc,
        vpc_subnets=vpc_subnets,
        environment=environment,
        memory_size=2048,
        timeout=180,
    )
    from_salesforce_bucket.grant_read_write(fn)
    rds_secret.grant_read(fn)

    definition = task_start_sf_update.next(task_complete_sf_update)

    log_group = logs.LogGroup(
        stack,
        id="ConsumeLogGroup",
        log_group_name="/sm/consume-dnswatch-queue",
        removal_policy=RemovalPolicy.DESTROY,
    )

    return sfn.StateMachine(
        stack,
        id=state_machine_name,
        state_machine_name=state_machine_name,
        definition_body=sfn.DefinitionBody.from_chainable(definition),
        logs=sfn.LogOptions(destination=log_group, level=sfn.LogLevel.ALL),
    )
