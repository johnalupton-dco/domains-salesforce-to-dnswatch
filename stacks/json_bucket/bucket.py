from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3


def create_s3_bucket(stack: Stack, bucket_name: str) -> s3.Bucket:
    artifact_bucket = s3.Bucket(
        stack,
        # stack creation can fail if id and name are same
        id=f"{bucket_name}-bucket-id",
        bucket_name=bucket_name,
        server_access_logs_prefix="access-logs/",
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        enforce_ssl=True,
        removal_policy=RemovalPolicy.DESTROY,
        auto_delete_objects=True,
    )
    return artifact_bucket
