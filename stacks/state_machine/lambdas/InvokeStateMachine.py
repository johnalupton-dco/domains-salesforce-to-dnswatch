import os

import boto3

STATE_MACHINE_ARN = os.environ["CDDO_STATE_MACHINE_ARN"]


def lambda_handler(event, context):
    print(f"Event\n{event}\n")

    message_body = event["Records"][0]["body"]
    client = boto3.client("stepfunctions")
    # response = client.start_execution(
    #     stateMachineArn=STATE_MACHINE_ARN, input=message_body
    # )
    # print(f"response:<{response}>")

    i = 0
    for e in event["Records"]:
        i = i + 1
        print(f"Event {i}", "*" * 60, f"\n{e}")
