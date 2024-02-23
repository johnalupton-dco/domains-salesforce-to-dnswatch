import datetime
import json
import os
import time
from typing import Any, Dict, List

import boto3
import pandas as pd
import requests
from cddo.utils.constants import (
    ENV_UPDATE_FROM_SALESFORCE_BUCKET,
    FLD_DOMAIN_RELATION,
    FLD_ORGANISATION,
    FLD_ORPHAN_ORGANISATION,
    FLD_SALESFORCE_CHANGE_FILES,
    FROM_SALESFORCE_FILESTUB,
    PS_SALESFORCE_CLIENT_ID,
    PS_SALESFORCE_CLIENT_SECRET,
    PS_SALESFORCE_DOMAIN,
    PS_SALESFORCE_EVENT_ROOT,
    PS_SALESFORCE_LAST_CHECKED,
    SALESFORCE_API_VERSION,
)
from cddo.utils.salesforce import get_access_token, query_to_df

ssm_client = boto3.client("ssm")
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
ddb_client = boto3.client("dynamodb")


TIMEOUT = 20
OUTPUT_BUCKET = os.environ[ENV_UPDATE_FROM_SALESFORCE_BUCKET]


work = dict()
work[FLD_ORGANISATION] = {
    "model": "organisation",
    "renamer": {"external_id__c": "object_id", "id": "salesforce_id"},
    "query": "select Id, Name,Description,Sector__c,Status__c,Type, CreatedDate, LastModifiedDate,  external_id__c from Account where",
}
work[FLD_DOMAIN_RELATION] = {
    "model": "domain",
    "renamer": {"external_id__c": "object_id", "id": "salesforce_id"},
    "query": "select Id, Name, Organisation__c, Parent_domain__c, Public_suffix__c, Organisation__r.Id, Organisation__r.Name, external_id__c \
    from Domain__c where",
}

work[FLD_ORPHAN_ORGANISATION] = {
    "model": "orphan",
    "renamer": {"external_id__c": "object_id", "id": "salesforce_id"},
    "query": "SELECT Id, Name, external_id__c FROM Account WHERE Id NOT IN (SELECT Organisation__c FROM Domain__c) and",
}


LAST_CHECKED_KEY = f"/{PS_SALESFORCE_EVENT_ROOT}/{PS_SALESFORCE_LAST_CHECKED}"


def date_now_as_sf_str() -> str:
    return str(datetime.datetime.now(datetime.UTC)).replace(" ", "T")


# def get_key(query_entity: str, now: str, file_count: int) -> str:
#     return f"{FROM_SALESFORCE_FILESTUB}-{query_entity}-{now}.{file_count:06d}.json"


# def put_file(data: Dict[str, Any], query_entity: str, now: str, file_count: int) -> str:
#     file_name = get_key(now=now, query_entity=query_entity, file_count=file_count)
#     s3_client.put_object(
#         Body=json.dumps(data),
#         Bucket=OUTPUT_BUCKET,
#         Key=file_name,
#     )
#     return file_name


# def get_updates_from_query(
#     domain: str,
#     headers: Dict[str, str],
#     now: str,
#     query: str,
#     query_entity: str,
# ) -> List[str]:
#     url = f"https://{domain}.my.salesforce.com/services/data/v{SALESFORCE_API_VERSION}/query"
#     params = {"q": f"{query} AND LastModifiedDate <= {now}"}
#     print(f"{query} AND LastModifiedDate <= {now}")

#     response = requests.get(url=url, params=params, headers=headers, timeout=TIMEOUT)
#     response_data = json.loads(response.content)

#     files_written = []

#     if type(response_data) is list and "errorCode" in response_data[0]:
#         print(json.dumps(response_data, indent=2, default=str))
#         raise RuntimeError(response_data)
#     else:
#         ddb_client.put_item(
#             TableName=query_entity,
#             Item={
#                 "as_at_datetime": {
#                     "S": str(
#                         datetime.datetime.fromisoformat(
#                             now.replace("T", " ")
#                         ).timestamp()
#                     )
#                 },
#                 "records": {"N": str(response_data["totalSize"])},
#             },
#         )

#         if response_data["totalSize"] != 0:
#             file_count = 1
#             files_written.append(
#                 put_file(
#                     data=response_data,
#                     now=now,
#                     query_entity=query_entity,
#                     file_count=file_count,
#                 )
#             )

#             while "nextRecordsUrl" in response_data:
#                 response = requests.get(
#                     url=f"https://{domain}.my.salesforce.com{response_data['nextRecordsUrl']}",
#                     headers=headers,
#                     timeout=TIMEOUT,
#                 )
#                 response_data = json.loads(response.content)

#                 file_count = file_count + 1
#                 files_written.append(
#                     put_file(
#                         data=response_data,
#                         now=now,
#                         query_entity=query_entity,
#                         file_count=file_count,
#                     )
#                 )

#         print(f"{response_data['totalSize']} records retrieved")
#     return files_written


# def lambda_handlerX(event, _context):
#     ssm_client.put_parameter(
#         Name=LAST_CHECKED_KEY,
#         Value=json.dumps(
#             {
#                 "organisation": "2023-01-01T00:00:01.000000+00:00",
#                 "domainRelation": "2023-01-01T00:00:01.000000+00:00",
#                 "orphanOrganisation": "2023-01-01T00:00:01.000000+00:00",
#             }
#         ),
#         Type="String",
#         Overwrite=True,
#     )

#     salesforce_last_checked_datetime = json.loads(
#         ssm_client.get_parameter(Name=LAST_CHECKED_KEY)["Parameter"]["Value"]
#     )

#     print(json.dumps(salesforce_last_checked_datetime, indent=2, default=str))

#     secret_value = json.loads(
#         secrets_client.get_secret_value(SecretId=PS_SALESFORCE_EVENT_ROOT)[
#             "SecretString"
#         ]
#     )

#     client_id = secret_value[PS_SALESFORCE_CLIENT_ID]
#     client_secret = secret_value[PS_SALESFORCE_CLIENT_SECRET]
#     domain = secret_value[PS_SALESFORCE_DOMAIN]

#     url = f"https://{domain}.my.salesforce.com/services/oauth2/token"
#     params = {
#         "grant_type": "client_credentials",
#         "client_id": client_id,
#         "client_secret": client_secret,
#     }

#     response = requests.post(url=url, params=params, timeout=TIMEOUT)
#     access_token = json.loads(response.content)["access_token"]
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#     }

#     now = date_now_as_sf_str()
#     # sleep to ensure no records missed at the snapshot time
#     time.sleep(2)
#     print(f"Collecting data at {now}")
#     files_written = dict()
#     for query_entity, query in work.items():
#         print(f"Processing {query_entity}")
#         files_written[query_entity] = get_updates_from_query(
#             domain=domain,
#             headers=headers,
#             now=now,
#             query=f"{query} LastModifiedDate > {salesforce_last_checked_datetime[query_entity]}",
#             query_entity=query_entity,
#         )

#         salesforce_last_checked_datetime[query_entity] = now

#     ssm_client.put_parameter(
#         Name=LAST_CHECKED_KEY,
#         Value=json.dumps(salesforce_last_checked_datetime),
#         Type="String",
#         Overwrite=True,
#     )

#     return {
#         ENV_UPDATE_FROM_SALESFORCE_BUCKET: OUTPUT_BUCKET,
#         FLD_SALESFORCE_CHANGE_FILES: json.dumps(files_written),
#     }


def lambda_handler(_event, _context):
    ssm_client.put_parameter(
        Name=LAST_CHECKED_KEY,
        Value=json.dumps(
            {
                "organisation": "2023-01-01T00:00:01.000000+00:00",
                "domainRelation": "2023-01-01T00:00:01.000000+00:00",
                "orphanOrganisation": "2023-01-01T00:00:01.000000+00:00",
            }
        ),
        Type="String",
        Overwrite=True,
    )

    salesforce_last_checked_datetime = json.loads(
        ssm_client.get_parameter(Name=LAST_CHECKED_KEY)["Parameter"]["Value"]
    )

    print(json.dumps(salesforce_last_checked_datetime, indent=2, default=str))

    domain, access_token = get_access_token(PS_SALESFORCE_EVENT_ROOT)

    now = date_now_as_sf_str()
    # sleep to ensure no records missed at the snapshot time
    time.sleep(2)

    print(f"Collecting data at {now}")

    files_written = dict()
    df_lookup = pd.DataFrame()
    for query_entity, info in work.items():
        print(f"Processing {query_entity}")

        query = f"{info['query']} LastModifiedDate > {salesforce_last_checked_datetime[query_entity]}"

        df = query_to_df(query=query, domain=domain, access_token=access_token)

        ddb_client.put_item(
            TableName=query_entity,
            Item={
                "as_at_datetime": {
                    "S": str(
                        datetime.datetime.fromisoformat(
                            now.replace("T", " ")
                        ).timestamp()
                    )
                },
                "records": {"N": str(len(df))},
            },
        )

        if len(df) != 0:
            df.columns = [x.lower() for x in df.columns]
            df["model"] = info["model"]
            df = df.rename(columns=info["renamer"])
            df_lookup = pd.concat(
                [df_lookup, df[["object_id", "salesforce_id", "model"]]]
            )

        key = f"{FROM_SALESFORCE_FILESTUB}-{query_entity}.csv"

        s3_client.put_object(
            Body=df.to_csv(encoding="utf-8", index=False, lineterminator="\n"),
            Bucket=OUTPUT_BUCKET,
            Key=key,
        )

        files_written[query_entity] = [key]

        salesforce_last_checked_datetime[query_entity] = now

    key = "salesforce_salesforceobject.csv"

    s3_client.put_object(
        Body=df_lookup.to_csv(encoding="utf-8", index=False, lineterminator="\n"),
        Bucket=OUTPUT_BUCKET,
        Key=key,
    )

    ssm_client.put_parameter(
        Name=LAST_CHECKED_KEY,
        Value=json.dumps(salesforce_last_checked_datetime),
        Type="String",
        Overwrite=True,
    )

    return {
        ENV_UPDATE_FROM_SALESFORCE_BUCKET: OUTPUT_BUCKET,
        FLD_SALESFORCE_CHANGE_FILES: files_written,
    }
