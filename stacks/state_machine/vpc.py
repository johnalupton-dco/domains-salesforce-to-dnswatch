from typing import Dict, Tuple

import aws_cdk.aws_ec2 as ec2
from aws_cdk import Stack
from cddo.utils.constants import ENV_RDS_SECRET_NAME

from stacks.constants import (
    FLD_CONTEXT_PRIVATESUBNETID1,
    FLD_CONTEXT_PRIVATESUBNETID2,
    FLD_CONTEXT_RDSSECRETNAME,
    FLD_CONTEXT_VPCID,
    FLD_CONTEXT_BASTIONHOSTSG,
)


def get_rds_vpc(
    stack: Stack, context: Dict[str, str]
) -> Tuple[ec2.Vpc, ec2.SecurityGroup, ec2.SubnetSelection, Dict[str, str]]:
    vpc = ec2.Vpc.from_lookup(
        stack,
        "RDSVpc",
        vpc_id=context[FLD_CONTEXT_VPCID],
    )
    security_group = ec2.SecurityGroup.from_security_group_id(
        stack, "BastionHost", context[FLD_CONTEXT_BASTIONHOSTSG]
    )
    subnet1 = ec2.PrivateSubnet.from_subnet_id(
        stack, "Subnet1RDS", context[FLD_CONTEXT_PRIVATESUBNETID1]
    )
    subnet2 = ec2.PrivateSubnet.from_subnet_id(
        stack, "Subnet2RDS", context[FLD_CONTEXT_PRIVATESUBNETID2]
    )

    environment = {
        ENV_RDS_SECRET_NAME: context[FLD_CONTEXT_RDSSECRETNAME],
    }

    return (
        vpc,
        security_group,
        ec2.SubnetSelection(subnets=[subnet1, subnet2]),
        environment,
    )
