from typing import List
import aws_cdk as cdk
import aws_cdk.aws_dynamodb as ddb

from cddo.utils.constants import FLD_ACCOUNT, FLD_DOMAIN_RELATION, FLD_ORPHAN_ACCOUNT


def create_dynamodb_tables(stack: cdk.Stack) -> List[ddb.TableV2]:
    tables = []
    for t in [FLD_ACCOUNT, FLD_DOMAIN_RELATION, FLD_ORPHAN_ACCOUNT]:
        tables.append(
            ddb.TableV2(
                scope=stack,
                id=t,
                table_name=t,
                partition_key=ddb.Attribute(
                    name="as_at_datetime", type=ddb.AttributeType.STRING
                ),
                contributor_insights=True,
                table_class=ddb.TableClass.STANDARD_INFREQUENT_ACCESS,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            )
        )

    return tables
