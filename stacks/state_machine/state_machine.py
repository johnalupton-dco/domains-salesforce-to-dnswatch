from typing import Dict, List, Optional


import aws_cdk.aws_dynamodb as ddb
import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk import Duration, RemovalPolicy, Stack
from cddo.utils import constants as cnst
from cddo.utils import lambdas

from stacks.constants import LL_CDDO_UTILS
from cddo.utils.constants import LL_REQUESTS, FLD_SALESFORCE_CHANGE_FILES

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


def _create_fargate_task(stack: Stack) -> tasks.EcsFargateLaunchTarget:
    vpc_name = "DefaultVpcForFargate"

    # vpc = ec2.Vpc.from_lookup(
    #     stack,
    #     id=vpc_name,
    #     is_default=True,
    #     # vpc_name=vpc_name,
    # )

    vpc = ec2.Vpc(
        stack,
        id=vpc_name,
        subnet_configuration=[
            ec2.SubnetConfiguration(
                cidr_mask=24,
                name="ingress",
                subnet_type=ec2.SubnetType.PUBLIC,
            ),
            ec2.SubnetConfiguration(
                cidr_mask=24,
                name="compute",
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
        ],
    )

    cluster_name = "ToDNSWatchFargateCluster"
    cluster = ecs.Cluster(
        scope=stack, id=cluster_name, cluster_name=cluster_name, vpc=vpc
    )

    task_family = "ToDNSWatchTask"
    task_definition = ecs.TaskDefinition(
        scope=stack,
        id=task_family,
        compatibility=ecs.Compatibility.FARGATE,
        # If you are using the Fargate launch type, this field is required
        # and you must use one of the following values, which determines your range of valid values for the memory parameter:
        # 256 (.25 vCPU) - Available memory values: 512 (0.5 GB), 1024 (1 GB), 2048 (2 GB)
        # 512 (.5 vCPU) - Available memory values: 1024 (1 GB), 2048 (2 GB), 3072 (3 GB), 4096 (4 GB)
        # 1024 (1 vCPU) - Available memory values: 2048 (2 GB), 3072 (3 GB), 4096 (4 GB), 5120 (5 GB), 6144 (6 GB), 7168 (7 GB), 8192 (8 GB)
        # 2048 (2 vCPU) - Available memory values: Between 4096 (4 GB) and 16384 (16 GB) in increments of 1024 (1 GB)
        # 4096 (4 vCPU) - Available memory values: Between 8192 (8 GB) and 30720 (30 GB) in increments of 1024 (1 GB)
        # 8192 (8 vCPU) - Available memory values: Between 16384 (16 GB) and 61440 (60 GB) in increments of 4096 (4 GB)
        # 16384 (16 vCPU) - Available memory values: Between 32768 (32 GB) and 122880 (120 GB) in increments of 8192 (8 GB)
        cpu="1024",
        # If using the Fargate launch type, this field is required and you must use one of the following values,
        # which determines your range of valid values for the cpu parameter:
        # 512 (0.5 GB),
        # 1024 (1 GB),
        # 2048 (2 GB)
        #  - Available cpu values: 256 (.25 vCPU) 1024 (1 GB), 2048 (2 GB), 3072 (3 GB), 4096 (4 GB)
        #  - Available cpu values: 512 (.5 vCPU) 2048 (2 GB), 3072 (3 GB), 4096 (4 GB), 5120 (5 GB), 6144 (6 GB), 7168 (7 GB), 8192 (8 GB)
        #  - Available cpu values: 1024 (1 vCPU) Between 4096 (4 GB) and 16384 (16 GB) in increments of 1024 (1 GB)
        #  - Available cpu values: 2048 (2 vCPU) Between 8192 (8 GB) and 30720 (30 GB) in increments of 1024 (1 GB)
        #  - Available cpu values: 4096 (4 vCPU) Between 16384 (16 GB) and 61440 (60 GB) in increments of 4096 (4 GB)
        #  - Available cpu values: 8192 (8 vCPU) Between 32768 (32 GB) and 122880 (120 GB) in increments of 8192 (8 GB)
        #  - Available cpu values: 16384 (16 vCPU)
        memory_mib="2048",
        runtime_platform=ecs.RuntimePlatform(
            cpu_architecture=ecs.CpuArchitecture.X86_64,
            operating_system_family=ecs.OperatingSystemFamily.LINUX,
        ),
    )

    repository = ecr.Repository.from_repository_name(
        scope=stack,
        id="ToDNSWatchRepo",
        repository_name="domains-dnswatch-bulk-update-from-salesforce",
    )

    container_definition = task_definition.add_container(
        id="ToDNSWatchContainer",
        image=ecs.ContainerImage.from_ecr_repository(
            repository=repository, tag="latest"
        ),
        logging=ecs.LogDrivers.aws_logs(
            stream_prefix="ToDNSWatch",
            mode=ecs.AwsLogDriverMode.NON_BLOCKING,
            log_group=logs.LogGroup(
                stack,
                id="ToDNSWatchLogGroup",
                log_group_name="/to-dns-watch/update-dnswatch",
                removal_policy=RemovalPolicy.DESTROY,
            ),
        ),
    )

    run_task = tasks.EcsRunTask(
        stack,
        "UpdateDNSWatch",
        integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        cluster=cluster,
        task_definition=task_definition,
        assign_public_ip=False,
        container_overrides=[
            tasks.ContainerOverride(
                container_definition=container_definition,
                environment=[
                    tasks.TaskEnvironmentVariable(
                        name=FLD_SALESFORCE_CHANGE_FILES,
                        value=sfn.JsonPath.string_at(
                            f"$.{FLD_SALESFORCE_CHANGE_FILES}"
                        ),
                    )
                ],
            )
        ],
        launch_target=tasks.EcsFargateLaunchTarget(
            platform_version=ecs.FargatePlatformVersion.LATEST
        ),
        propagated_tag_source=ecs.PropagatedTagSource.TASK_DEFINITION,
        result_path="$.Overrides.ContainerOverrides[0]",
    )

    return run_task


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
    secret_arns: List[str],
    json_bucket_arn: str,
    tables: List[ddb.TableV2],
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

    task_start_sf_update, fn = _create_lambda_task(
        stack=stack,
        task_name="GetSalesforceChanges",
        description="Query Salesforce with REST API to find updated data",
        policy_statements=policy_statements,
    )

    for t in tables:
        t.grant_write_data(fn)

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

    task_complete_sf_update, _ = _create_lambda_task(
        stack=stack,
        task_name="FinaliseSalesforceUpdate",
        description="Finalise Salesforce update",
        policy_statements=policy_statements,
    )

    fargate_task = _create_fargate_task(stack)

    definition = task_start_sf_update.next(fargate_task.next(task_complete_sf_update))

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
