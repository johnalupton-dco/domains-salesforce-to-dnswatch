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
poetry update
poetry add cddo-utils --source artifact
```

1. DONE parameters for url and version
2. DONE 3 params/json for
3. DONE get 3 new soql queries
4. bus
5. target email
6. update via lambda
7. DONE set parameter3 query times
8. DONE remove requets from lambda layers
