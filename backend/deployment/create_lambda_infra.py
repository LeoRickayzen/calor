"""
Create all infrastructure for the houses backend Lambda: ECR repo, IAM role,
Lambda function, and API Gateway HTTP API. Idempotent for ECR and IAM;
fails if Lambda already exists (use deploy_lambda.py to update code).

Uses the default AWS credential chain. Run from backend dir:
  python deployment/create_lambda_infra.py
  uv run python deployment/create_lambda_infra.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

from deployment._lambda_shared import (
    docker_build_and_push,
    ecr_repo_uri,
    resource_not_found,
)


_ECR_LAMBDA_PULL_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowLambdaPull",
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": ["ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"],
        }
    ],
}


def _ensure_ecr_repo(ecr, repo_name: str, region: str, account_id: str) -> str:
    try:
        ecr.create_repository(repositoryName=repo_name)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "RepositoryAlreadyExistsException":
            raise
    ecr.set_repository_policy(
        repositoryName=repo_name,
        policyText=json.dumps(_ECR_LAMBDA_PULL_POLICY),
    )
    return ecr_repo_uri(account_id, region, repo_name)


def _ensure_iam_role(
    iam,
    account_id: str,
    region: str,
    role_name: str,
    table_performance: str,
    table_dimension_index: str,
) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Execution role for houses-api Lambda",
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "EntityAlreadyExists":
            raise
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )
    dynamo_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:BatchGetItem",
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{table_performance}",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{table_dimension_index}",
                ],
            }
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{role_name}-dynamodb",
        PolicyDocument=json.dumps(dynamo_policy),
    )
    time.sleep(10)
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def _create_lambda(
    lambda_client,
    function_name: str,
    role_arn: str,
    image_uri: str,
    env_vars: dict[str, str],
    timeout: int,
    memory: int,
) -> None:
    lambda_client.create_function(
        FunctionName=function_name,
        Role=role_arn,
        Code={"ImageUri": image_uri},
        PackageType="Image",
        Timeout=timeout,
        MemorySize=memory,
        Environment={"Variables": env_vars},
    )


def _create_api_gateway(
    apigw,
    lambda_client,
    function_name: str,
    region: str,
    account_id: str,
    api_name: str,
) -> str:
    resp = apigw.create_api(Name=api_name, ProtocolType="HTTP")
    api_id = resp["ApiId"]
    api_endpoint = resp["ApiEndpoint"]
    lambda_arn = f"arn:aws:lambda:{region}:{account_id}:function:{function_name}"
    integ = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=lambda_arn,
        PayloadFormatVersion="2.0",
    )
    apigw.create_route(
        ApiId=api_id,
        RouteKey="$default",
        Target=f"integrations/{integ['IntegrationId']}",
    )
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=f"apigw-{api_id}",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*",
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceConflictException":
            raise
    return api_endpoint


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create ECR repo, IAM role, Lambda, and API Gateway for houses-api.",
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

    table_performance = os.environ.get(
        "HOUSES_TABLE_HOUSE_PRICE_PERFORMANCE",
        "house_price_performance",
    )
    table_dimension_index = os.environ.get(
        "HOUSES_TABLE_DIMENSION_INDEX",
        "dimension_index",
    )
    env_vars = {
        "HOUSES_TABLE_HOUSE_PRICE_PERFORMANCE": table_performance,
        "HOUSES_TABLE_DIMENSION_INDEX": table_dimension_index,
        "HOUSES_DYNAMODB_REGION": region,
    }

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    lambda_client = boto3.client("lambda", region_name=region)

    try:
        lambda_client.get_function(FunctionName=function_name)
        print(f"Lambda {function_name!r} already exists. Use deploy_lambda.py to update code.", file=sys.stderr)
        #sys.exit(1)
    except Exception as e:
        if not resource_not_found(e):
            raise

    ecr = boto3.client("ecr", region_name=region)
    repo_uri = _ensure_ecr_repo(ecr, ecr_repo, region, account_id)
    print("ECR repo ready:", repo_uri)

    iam = boto3.resource("iam")
    role_name = f"{function_name}-role"
    role_arn = _ensure_iam_role(
        iam.meta.client,
        account_id,
        region,
        role_name,
        table_performance,
        table_dimension_index,
    )
    print("IAM role ready:", role_arn)

    print("Building and pushing image...")
    image_uri = docker_build_and_push(backend_dir, repo_uri, "latest", region)

    #_create_lambda(lambda_client, function_name, role_arn, image_uri, env_vars, timeout=30, memory=256)
    print("Lambda created.")

    apigw = boto3.client("apigatewayv2", region_name=region)
    api_url = _create_api_gateway(apigw, lambda_client, function_name, region, account_id, api_name=function_name)
    print("API Gateway created.")
    print("Invoke URL:", api_url)


if __name__ == "__main__":
    main()
