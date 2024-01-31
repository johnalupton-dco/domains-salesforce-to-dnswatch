from typing import Dict, Any
import datetime
import json

import boto3
import requests

# logging
# client
# get secrets


ssm_client = boto3.client("ssm")
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

SALESFORCE_API_VERSION = "59.0"
FROM_SALESFORCE_FILESTUB = "update-from-salesforce"


PS_SALESFORCE_DOMAIN = "sf-domain"
PS_SALESFORCE_CLIENT_ID = "sf-client-id"
PS_SALESFORCE_CLIENT_SECRET = "sf-client-secret"
PS_SALESFORCE_EVENT_ROOT = "SalesforceToDNSWatch"
PS_SALESFORCE_SALESFORCE_LAST_CHECKED = "sf-last-checked-datetime"
PS_UPDATES_FROM_SALESFORCE_BUCKET = "updates-from-salesforce-bucket"

FLD_ACCOUNT = "account"
FLD_DOMAIN_RELATION = "domainRelation"
FLD_ORPHAN_ACCOUNT = "orphanAccount"


ACCOUNT_QUERY = "select Id, Name,Description,Sector__c,Status__c,Type, CreatedDate, LastModifiedDate from Account"
# ACCOUNT_QUERY = "select fields(all) from Account limit 5"

OUTPUT_BUCKET = ssm_client.get_parameter(
    Name=f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_UPDATES_FROM_SALESFORCE_BUCKET}"
)["Parameter"]["Value"]

LAST_CHECKED_KEY = (
    f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_SALESFORCE_SALESFORCE_LAST_CHECKED}"
)


def lambda_handler(event, _context):
    salesforce_last_checked_datetime = json.loads(
        ssm_client.get_parameter(Name=LAST_CHECKED_KEY)["Parameter"]["Value"]
    )
    print(json.dumps(salesforce_last_checked_datetime, indent=2, default=str))

    secret_value = json.loads(
        secrets_client.get_secret_value(SecretId=PS_SALESFORCE_EVENT_ROOT)[
            "SecretString"
        ]
    )

    client_id = secret_value[PS_SALESFORCE_CLIENT_ID]
    client_secret = secret_value[PS_SALESFORCE_CLIENT_SECRET]
    domain = secret_value[PS_SALESFORCE_DOMAIN]

    url = f"https://{domain}.my.salesforce.com/services/oauth2/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url=url, params=params)

    access_token = json.loads(response.content)["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    url = f"https://{domain}.my.salesforce.com/services/data/v{SALESFORCE_API_VERSION}/query"
    now = str(datetime.datetime.now(datetime.UTC)).replace(" ", "T")
    params = {
        "q": f"{ACCOUNT_QUERY} WHERE LastModifiedDate > {salesforce_last_checked_datetime[FLD_ACCOUNT]} AND LastModifiedDate <= {now}"
    }

    salesforce_last_checked_datetime[FLD_ACCOUNT] = now

    response = ssm_client.put_parameter(
        Name=LAST_CHECKED_KEY,
        Value=json.dumps(salesforce_last_checked_datetime),
        Type="String",
        Overwrite=True,
    )

    response = requests.get(url=url, params=params, headers=headers)

    # https://blog.sequin.io/pulling-past-the-limits-of-salesforces-updated-endpoint/
    response_data = json.loads(response.content)

    file_count = 0
    if "errorCode" in response_data:
        print(json.dumps(json.loads(response.content), indent=2, default=str))
    else:
        file_count = file_count + 1

        s3_client.put_object(
            Body=json.dumps(response_data),
            Bucket=OUTPUT_BUCKET,
            Key=f"{FROM_SALESFORCE_FILESTUB}-{now}.{file_count:06d}.json",
        )
