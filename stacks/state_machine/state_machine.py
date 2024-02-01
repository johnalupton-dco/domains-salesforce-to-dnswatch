from typing import Dict, List, Optional

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk import Duration, RemovalPolicy, Stack
from cddo.utils import constants as cnst
from cddo.utils import lambdas

from stacks.constants import LL_CDDO_UTILS
from cddo.utils.constants import LL_REQUESTS

layers = {}


def _get_lambda_layers(
    stack: Stack, layer_names: List[str]
) -> Dict[str, lambda_.LayerVersion]:
    bucket_name = cnst.LL_LAYER_BUCKET
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
) -> tasks.LambdaInvoke:
    lambda_function = lambdas.create_lambda(
        stack=stack,
        name=task_name,
        description=description,
        timeout=60,
        source_folder="./lambdas",
        create_log_group=True,
        lambda_layers=[l for k, l in layers.items()],
    )
    if policy_statements:
        for p in policy_statements:
            lambda_function.add_to_role_policy(p)

    return tasks.LambdaInvoke(
        payload_response_only=True,
        scope=stack,
        id=task_name,
        state_name=task_name,
        retry_on_service_exceptions=False,
        lambda_function=lambda_function,
    )


def create_queue_consume_state_machine(
    stack: cdk.Stack, secret_arns: List[str], json_bucket_arn: str
) -> sfn.StateMachine:
    _get_lambda_layers(stack, [LL_CDDO_UTILS, LL_REQUESTS])

    state_machine_name = "SendSalesforceUpdatesToDNSWatch"

    wait_x_secs = 15
    wait_x_mins = sfn.Wait(
        stack,
        id="WaitForUpdate",
        state_name="Wait",
        time=sfn.WaitTime.duration(Duration.seconds(wait_x_secs)),
    )

    policy_statements = []
    policy_statements.append(
        iam.PolicyStatement(
            actions=[
                "secretsmanager:GetSecretValue",
            ],
            effect=iam.Effect.ALLOW,
            resources=[secret_arns[0]],
            sid="SecretAccess",
        )
    )

    policy_statements.append(
        iam.PolicyStatement(
            actions=[
                "ssm:GetParameter",
            ],
            effect=iam.Effect.ALLOW,
            resources=secret_arns[1:],
            sid="ParamsGet",
        )
    )

    policy_statements.append(
        iam.PolicyStatement(
            actions=[
                "ssm:PutParameter",
            ],
            effect=iam.Effect.ALLOW,
            resources=[secret_arns[1]],
            sid="ParamsPut",
        )
    )
    ps = iam.PolicyStatement(
        actions=["s3:PutObject"],
        effect=iam.Effect.ALLOW,
        resources=[f"{json_bucket_arn}/*"],
        sid="S3BucketPut",
    )

    policy_statements.append(ps)

    task_start_sf_update = _create_lambda_task(
        stack=stack,
        task_name="GetSalesforceChanges",
        description="Query Salesforce with REST API to find updated data",
        policy_statements=policy_statements,
    )

    policy_statements = []
    policy_statements.append(
        iam.PolicyStatement(
            actions=[
                "ssm:GetParameter",
            ],
            effect=iam.Effect.ALLOW,
            resources=secret_arns[1:],
            sid="ParamsGet",
        )
    )

    policy_statements.append(
        iam.PolicyStatement(
            actions=[
                "s3:PutObject",
                "s3:GetObject",
                "s3:CopyObject",
                "s3:DeleteObject",
            ],
            effect=iam.Effect.ALLOW,
            resources=[f"{json_bucket_arn}/*"],
            sid="S3BucketPut",
        )
    )

    task_complete_sf_update = _create_lambda_task(
        stack=stack,
        task_name="FinaliseSalesforceUpdate",
        description="Finalise Salesforce update",
        policy_statements=policy_statements,
    )

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
