import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct
from stacks.ssm_and_secrets import create_secrets_and_params
from stacks.constants import (
    FLD_CONTEXT_SALESFORCE_CONSUMER_KEY,
    FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET,
    FLD_CONTEXT_SCHEDULE_EXPRESSION,
)
from stacks.state_machine import create_queue_consume_state_machine
from stacks.eventbridge import create_schedule


class ToDNSWatch(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        client_id: str,
        client_secret: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        create_secrets_and_params(
            stack=self, client_id=client_id, client_secret=client_secret
        )

        sm = create_queue_consume_state_machine(stack=self)

        create_schedule(
            stack=self,
            schedule_expression=app.node.get_context(FLD_CONTEXT_SCHEDULE_EXPRESSION),
            state_machine=sm,
        )


app = cdk.App()
ToDNSWatch(
    app,
    "ToDNSWatch",
    client_id=app.node.get_context(FLD_CONTEXT_SALESFORCE_CONSUMER_KEY),
    client_secret=app.node.get_context(FLD_CONTEXT_SALESFORCE_CONSUMER_SECRET),
)

app.synth()
