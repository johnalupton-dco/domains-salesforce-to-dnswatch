from typing import Any, Dict

import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct

from stacks.constants import (FLD_CONTEXT_SALESFORCE,
                              FLD_CONTEXT_SCHEDULE_EXPRESSION,
                              FLD_CONTEXT_UPDATES_FROM_SF_BUCKET)
from stacks.eventbridge import create_schedule
from stacks.json_bucket import create_s3_bucket
from stacks.ssm_and_secrets import create_secrets_and_params
from stacks.state_machine import create_queue_consume_state_machine


class ToDNSWatch(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        salesforce_context: Dict[str, Any],
        from_salesforce_bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        secret_arns = create_secrets_and_params(
            stack=self,
            salesforce_context=salesforce_context,
            from_salesforce_bucket_name=from_salesforce_bucket_name,
        )

        json_bucket = create_s3_bucket(
            stack=self, bucket_name=from_salesforce_bucket_name
        )

        sm = create_queue_consume_state_machine(
            stack=self, secret_arns=secret_arns, json_bucket_arn=json_bucket.bucket_arn
        )

        create_schedule(
            stack=self,
            schedule_expression=app.node.get_context(FLD_CONTEXT_SCHEDULE_EXPRESSION),
            state_machine=sm,
        )


app = cdk.App()
ToDNSWatch(
    app,
    "ToDNSWatch",
    salesforce_context=app.node.get_context(FLD_CONTEXT_SALESFORCE),
    from_salesforce_bucket_name=app.node.get_context(
        FLD_CONTEXT_UPDATES_FROM_SF_BUCKET
    ),
)

app.synth()
