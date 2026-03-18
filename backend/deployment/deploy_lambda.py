"""
Build the Lambda image, push to ECR, and update the Lambda function code.
Assumes infrastructure already exists (run create_lambda_infra.py first).

Run from backend dir: python deployment/deploy_lambda.py
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3

from deployment._lambda_shared import (
    docker_build_and_push,
    ecr_repo_uri,
    resource_not_found,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build, push image to ECR, and update Lambda code.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION") or os.environ.get("HOUSES_DYNAMODB_REGION") or "us-east-1",
        help="AWS region",
    )
    parser.add_argument(
        "--function-name",
        default=os.environ.get("HOUSES_LAMBDA_FUNCTION_NAME") or "houses-api",
        help="Lambda function name",
    )
    parser.add_argument(
        "--ecr-repo",
        default=os.environ.get("HOUSES_ECR_REPO") or "houses-backend",
        help="ECR repository name",
    )
    parser.add_argument(
        "--account-id",
        required=True,
        help="AWS account ID (12-digit).",
    )
    args = parser.parse_args()
    region = args.region
    function_name = args.function_name
    ecr_repo = args.ecr_repo
    account_id = args.account_id

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_uri = ecr_repo_uri(account_id, region, ecr_repo)
    lambda_client = boto3.client("lambda", region_name=region)

    try:
        lambda_client.get_function(FunctionName=function_name)
    except Exception as e:
        if resource_not_found(e):
            print("Lambda does not exist. Run create_lambda_infra.py first.", file=sys.stderr)
            sys.exit(1)
        raise

    print("Building and pushing image...")
    image_uri = docker_build_and_push(backend_dir, repo_uri, "latest", region)
    lambda_client.update_function_code(FunctionName=function_name, ImageUri=image_uri)
    print("Lambda code updated.")


if __name__ == "__main__":
    main()
