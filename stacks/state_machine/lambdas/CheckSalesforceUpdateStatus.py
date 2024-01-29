from random import randrange


def lambda_handler(event, _context):
    test = randrange(10)
    response = {"SalesforceUpdateStatus": {}}

    print(f"test <{test}>")
    if test > 7:
        response["SalesforceUpdateStatus"] = {"status": "ok"}
    else:
        response["SalesforceUpdateStatus"] = {"status": "ko"}

    print(f"response <{response}>")

    return response
