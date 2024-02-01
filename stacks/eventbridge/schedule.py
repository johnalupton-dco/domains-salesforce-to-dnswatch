import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as stepfunctions
from aws_cdk import Stack
from aws_cdk import aws_iam as iam


def create_schedule(
    stack: Stack,
    schedule_expression: str,
    state_machine: stepfunctions.StateMachine,
    **kwargs,
) -> None:
    def SnapshotSchedulerRole(stack: Stack):
        role_name = f"{stack.stack_name}-SnapshotSchedulerRole"

        role = iam.Role(
            stack,
            id=role_name,
            role_name=role_name,
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("events.amazonaws.com"),
                iam.ServicePrincipal("states.amazonaws.com"),
            ),
        )

        statement_states = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["states:startexecution"],
            resources=["*"],
        )

        statement_event = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["events:DescribeRule"],
            resources=["*"],
        )

        policy_document = iam.PolicyDocument(
            statements=[statement_states, statement_event]
        )

        policy_name = f"{stack.stack_name}-InvokeCloudWatchEvent"

        policy = iam.Policy(
            stack, id=policy_name, policy_name=policy_name, document=policy_document
        )

        role.attach_inline_policy(policy=policy)

        return role

    def step_function_lambda_execution_role(stack, topic: sns.Topic):
        role_name = f"{stack.stack_name}-StepFnLambdaExecutionRole"

        role = iam.Role(
            stack,
            id=role_name,
            role_name=role_name,
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("states.amazonaws.com"),
            ),
        )

        statement_states = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["lambda:InvokeFunction"],
            resources=["*"],
        )

        statement_event = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sns:Publish"],
            resources=[topic.topic_arn],
        )

        policy_document = iam.PolicyDocument(
            statements=[statement_states, statement_event]
        )

        policy_name = f"{stack.stack_name}-InvokeCallbackLambda"

        policy = iam.Policy(
            stack, id=policy_name, policy_name=policy_name, document=policy_document
        )

        role.attach_inline_policy(policy=policy)

        return role

    snapshot_scheduler_role = SnapshotSchedulerRole(stack)

    rule_name = "CheckFromSalesforceUpdatesStateMachine"

    target = targets.SfnStateMachine(state_machine, role=snapshot_scheduler_role)

    events.Rule(
        stack,
        id=rule_name,
        enabled=True,
        description="To Trigger the Step Function for Snapshot creation",
        rule_name=rule_name,
        schedule=events.Schedule.expression(schedule_expression),
        targets=[target],
    )
