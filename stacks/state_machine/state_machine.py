import aws_cdk as cdk
import aws_cdk.aws_logs as logs
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk import Duration, RemovalPolicy
from cddo.utils import lambdas


def _create_lambda_task(
    stack: cdk.Stack, task_name: str, description: str
) -> tasks.LambdaInvoke:
    return tasks.LambdaInvoke(
        payload_response_only=True,
        scope=stack,
        id=task_name,
        state_name=task_name,
        retry_on_service_exceptions=False,
        lambda_function=lambdas.create_lambda(
            stack=stack,
            name=task_name,
            description=description,
            timeout=60,
            source_folder="./lambdas",
            create_log_group=True,
        ),
    )


def create_queue_consume_state_machine(
    stack: cdk.Stack,
) -> sfn.StateMachine:
    state_machine_name = "SendSalesforceUpdatesToDNSWatch"

    wait_x_secs = 15
    wait_x_mins = sfn.Wait(
        stack,
        id="WaitForUpdate",
        state_name="Wait",
        time=sfn.WaitTime.duration(Duration.seconds(wait_x_secs)),
    )

    task_start_sf_update = _create_lambda_task(
        stack=stack,
        task_name="GetSalesforceChanges",
        description="Query Salesforce with REST API to find updated data",
    )
    task_check_sf_update_status = _create_lambda_task(
        stack=stack,
        task_name="CheckSalesforceUpdateStatus",
        description="Check status of Salesforce update",
    )
    task_complete_sf_update = _create_lambda_task(
        stack=stack,
        task_name="FinaliseSalesforceUpdate",
        description="Finalise Salesforce update",
    )

    definition = task_start_sf_update.next(task_check_sf_update_status).next(
        sfn.Choice(stack, id="HasSalesforceUpdateCompleted")
        .when(
            sfn.Condition.not_(
                sfn.Condition.string_equals("$.SalesforceUpdateStatus.status", "ok")
            ),
            wait_x_mins.next(task_check_sf_update_status),
        )
        .otherwise(task_complete_sf_update)
    )

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
