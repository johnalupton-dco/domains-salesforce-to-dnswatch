import json

import boto3
import requests

# logging
# client
# get secrets


ssm_client = boto3.client("ssm")
secrets_client = boto3.client("secretsmanager")

DOMAIN = "laspitiusaslimited-dev-ed.develop"


PS_SF_CLIENT_ID = "sf-client-id"
PS_SF_CLIENT_SECRET = "sf-client-secret"
PS_SF_EVENT_ROOT = "SalesforceToDNSWatch"
PS_SF_SALESFORCE_LAST_CHECKED = "sf-last-checked-datetime"


def lambda_handler(event, _context):
    salesforce_last_checked_datetime = ssm_client.get_parameter(
        Name=f"/{PS_SF_EVENT_ROOT}/{PS_SF_SALESFORCE_LAST_CHECKED}"
    )["Parameter"]["Value"]

    secret_value = json.loads(
        secrets_client.get_secret_value(SecretId=PS_SF_EVENT_ROOT)["SecretString"]
    )

    client_id = secret_value[PS_SF_CLIENT_ID]
    client_secret = secret_value[PS_SF_CLIENT_SECRET]

    url = f"https://{DOMAIN}.my.salesforce.com/services/oauth2/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    try:
        response = requests.post(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err) from err

    access_token = json.loads(response.content)["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    url = f"https://{DOMAIN}.my.salesforce.com/services/data/v59.0/query"
    params = {"q": "SELECT AccountNumber FROM Account LIMIT 200"}

    try:
        response = requests.get(
            url,
            params=params,
            timeout=10,
            headers=headers,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err) from err

    # https://blog.sequin.io/pulling-past-the-limits-of-salesforces-updated-endpoint/
    print(json.dumps(json.loads(response.content), indent=2, default=str))
