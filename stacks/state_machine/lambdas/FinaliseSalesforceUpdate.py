import json
import os

import boto3
from cddo.utils.constants import (ENV_UPDATE_FROM_SALESFORCE_BUCKET,
                                  FLD_SALESFORCE_CHANGE_FILES)

OUTPUT_BUCKET = os.environ[ENV_UPDATE_FROM_SALESFORCE_BUCKET]


def lambda_handler(event, _context):
    s3 = boto3.resource("s3")
    input_files = json.loads(event[FLD_SALESFORCE_CHANGE_FILES])

    for query_entity, files in input_files.items():
        print(f"Processing {query_entity}")
        for file in files:
            s3.Object(OUTPUT_BUCKET, f"archive/{query_entity}/{file}").copy_from(
                CopySource=f"{OUTPUT_BUCKET}/{file}"
            )
            s3.Object(OUTPUT_BUCKET, file).delete()
