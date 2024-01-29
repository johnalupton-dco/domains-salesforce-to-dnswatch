from typing import List

import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from cddo.utils import lambdas


def _get_lambda_assume_role(
    stack: Stack, name: str, statements: List[iam.PolicyStatement]
) -> iam.Role:
    principal = iam.ServicePrincipal("lambda.amazonaws.com")
    role_name = f"{stack.stack_name}-{name}Role"
    role = iam.Role(
        stack,
        id=role_name,
        role_name=role_name,
        assumed_by=principal,
    )

    policy_document = iam.PolicyDocument(statements=statements)

    policy_name = f"{stack.stack_name}-{name}Policy"

    policy = iam.Policy(
        stack, id=policy_name, policy_name=policy_name, document=policy_document
    )

    policy.attach_to_role(role=role)

    return role


def _get_lambda_invoke_step_function_role(stack: Stack, function_name: str) -> iam.Role:
    statement_states = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=["states:*"],
        resources=["*"],
    )

    statement_logs = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            "logs:DescribeLogStreams",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:CreateLogGroup",
        ],
        resources=["*"],
    )

    statement_sqs = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
        resources=["*"],
    )

    statements = [statement_states, statement_logs, statement_sqs]

    return _get_lambda_assume_role(stack, function_name, statements)


def create_invoke_lambda(
    stack: cdk.Stack, state_machine_arn: str, to_sf_queue: sqs.Queue
) -> None:
    function_name = "InvokeStateMachine"
    fn = lambdas.create_lambda(
        stack=stack,
        name=function_name,
        description="Invoke state machine to bulk update Salesforce",
        timeout=60,
        source_folder="./lambdas",
        create_log_group=True,
        environment={"CDDO_STATE_MACHINE_ARN": state_machine_arn},
        role=_get_lambda_invoke_step_function_role(
            stack=stack, function_name=function_name
        ),
    )

    event_source = SqsEventSource(to_sf_queue)

    fn.add_event_source(event_source)
