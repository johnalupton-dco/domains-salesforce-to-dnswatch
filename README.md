# domains-salesforce-to-dnswatch

Constructs to facilitate update of DNSWatch from Salesforce

```toml
[[tool.poetry.source]]
name = "artifact"
url = "https://cddo-custom-package-domain-637696396477.d.codeartifact.eu-north-1.amazonaws.com/pypi/cddo-utils/simple"
priority="supplemental"
```

```bash
export POETRY_HTTP_BASIC_ARTIFACT_PASSWORD=$(aws codeartifact get-authorization-token --domain cddo --query authorizationToken --output text --profile sb)
export POETRY_HTTP_BASIC_ARTIFACT_USERNAME=aws
poetry add cddo-utils --source artifact
poetry update
```

1. parameters for url and version
2. 3 params/json for
3. get 3 new soql queries
4. bus
5. target email
6. update via lambda
7. set parameter3 query times
8. remove requets from lambda layers

ACCOUNT_QUERY = "select Id, Name,Description,Sector**c,Status**c,Type, CreatedDate, LastModifiedDate from Account"
DOMAIN_RELATION_QUERY = "select Id, Name, Organisation**c, Parent_domain**c, Public_suffix**c, Organisation**r.Id, Organisation**r.Name from Domain**c"
ACCOUNT_WITHOUT_DOMAIN_QUERY = "SELECT Id, Name FROM Account WHERE Id NOT IN (SELECT Organisation**c FROM Domain**c)"
