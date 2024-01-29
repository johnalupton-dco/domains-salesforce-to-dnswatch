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
