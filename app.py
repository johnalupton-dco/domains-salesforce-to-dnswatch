import os
from typing import Any, Dict

import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct

from stacks.constants import (
    FLD_CONTEXT_PROFILE,
    FLD_CONTEXT_SCHEDULE_EXPRESSION,
    FLD_CONTEXT_UPDATES_FROM_SF_BUCKET,
)
from stacks.dynamodb import create_dynamodb_tables
from stacks.eventbridge import create_schedule
from stacks.json_bucket import create_s3_bucket
from stacks.ssm_and_secrets import create_secrets_and_params
from stacks.state_machine import create_queue_consume_state_machine


class ToDNSWatch(Stack):
    """
    1. Create secrets for salesforce access and parameter for "last updated" variables
    2. Create s3 bucket to put json files extracted from salesforce
    3. Create dynamodb tables to store import stats (number of records of each type pulled from salesforce)
    4. Create state machine to pull data from salesforce to DNSWatch - steps are:
     * State 1: GetSalesforceChanges - lambda function to call salesforce api to get changes and save them to json files
     * State 2: domains-dnswatch-bulk-update-from-salesforce - farget task to process files
     * State 3: FinaliseSalesforceUpdate - lambda to archive processed files
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        context: Dict[str, Any],
        from_salesforce_bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ARNs for secrets and parameters - needed to grant permissions to lambda functions for read/write
        salesforce_secret, last_checked_param = create_secrets_and_params(
            stack=self,
            context=context,
        )

        # Bucket to put data read from salesforce REST API
        from_salesforce_bucket = create_s3_bucket(
            stack=self, bucket_name=from_salesforce_bucket_name
        )

        # dynamodb tables to store summary stats of each run
        tables = create_dynamodb_tables(stack=self)

        # State machine to run all steps in the update run on an EventBridge schedule
        sm = create_queue_consume_state_machine(
            stack=self,
            from_salesforce_bucket=from_salesforce_bucket,
            salesforce_secret=salesforce_secret,
            last_checked_param=last_checked_param,
            tables=tables,
            profile=app.node.get_context(FLD_CONTEXT_PROFILE),
            context=context,
        )

        # Eventbridge schedule to run update
        create_schedule(
            stack=self,
            schedule_expression=app.node.get_context(FLD_CONTEXT_SCHEDULE_EXPRESSION),
            state_machine=sm,
        )


app = cdk.App()

profile = app.node.get_context(FLD_CONTEXT_PROFILE)
bucket = app.node.get_context(FLD_CONTEXT_UPDATES_FROM_SF_BUCKET)


ToDNSWatch(
    app,
    "ToDNSWatch",
    context=app.node.get_context(profile),
    from_salesforce_bucket_name=f"{bucket}-{profile}",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEPLOY_ACCOUNT", os.environ["CDK_DEFAULT_ACCOUNT"]),
        region=os.environ.get("CDK_DEPLOY_REGION", os.environ["CDK_DEFAULT_REGION"]),
    ),
)

app.synth()
