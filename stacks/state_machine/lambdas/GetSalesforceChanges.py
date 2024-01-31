import datetime
from typing import Dict, Any
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


TIMEOUT = 20


PS_SALESFORCE_DOMAIN = "sf-domain"
PS_SALESFORCE_CLIENT_ID = "sf-client-id"
PS_SALESFORCE_CLIENT_SECRET = "sf-client-secret"
PS_SALESFORCE_EVENT_ROOT = "SalesforceToDNSWatch"
PS_SALESFORCE_SALESFORCE_LAST_CHECKED = "sf-last-checked-datetime"
PS_UPDATES_FROM_SALESFORCE_BUCKET = "updates-from-salesforce-bucket"

FLD_ACCOUNT = "account"
FLD_DOMAIN_RELATION = "domainRelation"
FLD_ORPHAN_ACCOUNT = "orphanAccount"


work = dict()
work[
    FLD_ACCOUNT
] = "select Id, Name,Description,Sector__c,Status__c,Type, CreatedDate, LastModifiedDate from Account where"
work[
    FLD_DOMAIN_RELATION
] = "select Id, Name, Organisation__c, Parent_domain__c, Public_suffix__c, Organisation__r.Id, Organisation__r.Name from Domain__c where"

work[
    FLD_ORPHAN_ACCOUNT
] = "SELECT Id, Name FROM Account WHERE Id NOT IN (SELECT Organisation__c FROM Domain__c) and"


OUTPUT_BUCKET = ssm_client.get_parameter(
    Name=f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_UPDATES_FROM_SALESFORCE_BUCKET}"
)["Parameter"]["Value"]

LAST_CHECKED_KEY = (
    f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_SALESFORCE_SALESFORCE_LAST_CHECKED}"
)


def date_now_as_sf_str() -> str:
    return str(datetime.datetime.now(datetime.UTC)).replace(" ", "T")


def get_key(query_entity: str, now: str, file_count: int) -> str:
    return f"{FROM_SALESFORCE_FILESTUB}-{query_entity}-{now}.{file_count:06d}.json"


def put_file(data: Dict[str, Any], query_entity: str, now: str, file_count: int) -> str:
    s3_client.put_object(
        Body=json.dumps(data),
        Bucket=OUTPUT_BUCKET,
        Key=get_key(now=now, query_entity=query_entity, file_count=file_count),
    )


def get_updates_from_query(
    domain: str,
    headers: Dict[str, str],
    now: str,
    query: str,
    query_entity: str,
):
    url = f"https://{domain}.my.salesforce.com/services/data/v{SALESFORCE_API_VERSION}/query"
    params = {"q": f"{query} AND LastModifiedDate <= {now}"}

    response = requests.get(url=url, params=params, headers=headers, timeout=TIMEOUT)
    response_data = json.loads(response.content)

    if "errorCode" in response_data:
        print(json.dumps(json.loads(response.content), indent=2, default=str))
    else:
        if response_data["totalSize"] != 0:
            file_count = 1
            put_file(
                data=response_data,
                now=now,
                query_entity=query_entity,
                file_count=file_count,
            )

            while "nextRecordsUrl" in response_data:
                response = requests.get(
                    url=f"https://{domain}.my.salesforce.com{response_data['nextRecordsUrl']}",
                    headers=headers,
                    timeout=TIMEOUT,
                )
                response_data = json.loads(response.content)

                file_count = file_count + 1
                put_file(
                    data=response_data,
                    now=now,
                    query_entity=query_entity,
                    file_count=file_count,
                )

        print(f"{response_data['totalSize']} records retrieved")


def lambda_handler(event, _context):
    salesforce_last_checked_datetime = {
        FLD_ACCOUNT: "2024-01-01T00:00:00Z",
        FLD_DOMAIN_RELATION: "2024-01-01T00:00:00Z",
        FLD_ORPHAN_ACCOUNT: "2024-01-01T00:00:00Z",
    }
    ssm_client.put_parameter(
        Name=LAST_CHECKED_KEY,
        Value=json.dumps(salesforce_last_checked_datetime),
        Type="String",
        Overwrite=True,
    )

    salesforce_last_checked_datetime = json.loads(
        ssm_client.get_parameter(Name=LAST_CHECKED_KEY)["Parameter"]["Value"]
    )

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

    response = requests.post(url=url, params=params, timeout=TIMEOUT)
    access_token = json.loads(response.content)["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    now = date_now_as_sf_str()
    for query_entity, query in work.items():
        get_updates_from_query(
            domain=domain,
            headers=headers,
            now=now,
            query=f"{query} LastModifiedDate > {salesforce_last_checked_datetime[query_entity]}",
            query_entity=query_entity,
        )
        salesforce_last_checked_datetime[query_entity] = now

    ssm_client.put_parameter(
        Name=LAST_CHECKED_KEY,
        Value=json.dumps(salesforce_last_checked_datetime),
        Type="String",
        Overwrite=True,
    )
