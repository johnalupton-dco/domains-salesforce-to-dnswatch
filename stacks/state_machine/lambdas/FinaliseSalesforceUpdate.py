import json
import os
import io
import pandas as pd
import boto3
from cddo.utils.constants import (
    ENV_UPDATE_FROM_SALESFORCE_BUCKET,
    FLD_SALESFORCE_CHANGE_FILES,
)

OUTPUT_BUCKET = os.environ[ENV_UPDATE_FROM_SALESFORCE_BUCKET]
s3_client = boto3.client("s3")


def lambda_handler(event, _context):
    s3 = boto3.resource("s3")
    input_files = event[FLD_SALESFORCE_CHANGE_FILES]

    for query_entity, files in input_files.items():
        print(f"Processing {query_entity}")
        for file in files:
            s3.Object(OUTPUT_BUCKET, f"archive/{file}").copy_from(
                CopySource=f"{OUTPUT_BUCKET}/{file}"
            )
            s3.Object(OUTPUT_BUCKET, file).delete()

    df_lookup = pd.read_csv(
        io.StringIO(
            s3_client.get_object(
                Bucket=OUTPUT_BUCKET, Key="salesforce_salesforceobject.csv"
            )["Body"]
            .read()
            .decode("utf-8")
        ),
        sep=",",
    )

    df_lookup = df_lookup.loc[df_lookup["model"].isin(["domain", "organisation"]), :]
    df_lookup = df_lookup.dropna(subset=["object_id"])
    df_lookup["content_id_type"] = 0
    df_lookup["batch"] = pd.NA

    print(df_lookup)
