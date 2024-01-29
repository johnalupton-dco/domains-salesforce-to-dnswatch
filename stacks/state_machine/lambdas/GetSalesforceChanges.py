import json


def lambda_handler(event, _context):
    print(json.dumps(event, indent=2, default=str))
    print("Starting bulk update")
