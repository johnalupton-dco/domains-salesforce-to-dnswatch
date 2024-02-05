import boto3
import json


from cddo.utils.constants import (
    PS_SALESFORCE_EVENT_ROOT,
    PS_UPDATES_FROM_SALESFORCE_BUCKET,
    FLD_SALESFORCE_CHANGE_FILES,
)


ssm_client = boto3.client("ssm")

OUTPUT_BUCKET = ssm_client.get_parameter(
    Name=f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_UPDATES_FROM_SALESFORCE_BUCKET}"
)["Parameter"]["Value"]


def lambda_handler(event, _context):
    s3 = boto3.resource("s3")
    input_files = json.loads(event[FLD_SALESFORCE_CHANGE_FILES])

    for k in input_files:
        s3.Object(OUTPUT_BUCKET, f"archive/{k}").copy_from(
            CopySource=f"{OUTPUT_BUCKET}/{k}"
        )
        s3.Object(OUTPUT_BUCKET, k).delete()
