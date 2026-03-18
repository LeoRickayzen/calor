"""Shared helpers for create_lambda_infra and deploy_lambda."""

from __future__ import annotations

import subprocess

import boto3
from botocore.exceptions import ClientError


def resource_not_found(e: Exception) -> bool:
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code") == "ResourceNotFoundException"
    return False


def get_account_id(region: str) -> str:
    sts = boto3.client("sts", region_name=region)
    return sts.get_caller_identity()["Account"]


def run(cmd: list[str], cwd: str | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def docker_build_and_push(
    backend_dir: str,
    repo_uri: str,
    tag: str,
    region: str,
) -> str:
    image_uri = f"{repo_uri}:{tag}"
    run(["docker", "build", "--platform", "linux/amd64", "-t", image_uri, "."], cwd=backend_dir)
    result = subprocess.run(
        ["aws", "ecr", "get-login-password", "--region", region],
        capture_output=True,
        text=True,
        check=True,
        cwd=backend_dir,
    )
    password = result.stdout.strip()
    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", repo_uri.split("/")[0]],
        input=password.encode(),
        capture_output=True,
        check=True,
        cwd=backend_dir,
    )
    run(["docker", "push", image_uri], cwd=backend_dir)
    return image_uri


def ecr_repo_uri(account_id: str, region: str, repo_name: str) -> str:
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{repo_name}"
