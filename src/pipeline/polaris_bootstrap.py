"""Configure the local Polaris server. Safe to re-run.

Steps (all through the Polaris management REST API, as the bootstrap root user):
  1. Wait for Polaris to be healthy.
  2. Create an INTERNAL catalog `nyc_taxi_lake` whose storage is
     s3://<bucket>/warehouse/, accessed by Polaris via the nyc-taxi-polaris role.
  3. Create a principal `spark_etl` (the identity Spark uses), a principal role
     `etl_role`, and a catalog role `etl_content_manager` with
     CATALOG_MANAGE_CONTENT (create/read/write namespaces and tables; no
     catalog admin rights). Wire principal -> principal role -> catalog role.
  4. Write the principal's client id/secret to .polaris.env (gitignored).

Run: python -m pipeline.polaris_bootstrap
"""
from __future__ import annotations

import sys
import time

import requests

from pipeline.config import REPO_ROOT, load_config, require

PRINCIPAL = "spark_etl"
PRINCIPAL_ROLE = "etl_role"
CATALOG_ROLE = "etl_content_manager"


def wait_for_health(url: str, timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=3).status_code == 200:
                print("Polaris is healthy")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit(f"Polaris not healthy at {url} after {timeout_s}s. Is `make polaris-up` running?")


def root_token(catalog_uri: str, client_id: str, client_secret: str) -> str:
    resp = requests.post(
        f"{catalog_uri}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def call(method: str, url: str, token: str, body: dict | None = None, ok_conflict: bool = True) -> requests.Response:
    resp = requests.request(method, url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=15)
    if resp.status_code == 409 and ok_conflict:
        print(f"  already exists: {method} {url.rsplit('/', 2)[-2:]}")
        return resp
    if resp.status_code >= 400:
        sys.exit(f"{method} {url} failed: {resp.status_code} {resp.text}")
    return resp


def main() -> None:
    cfg = load_config()
    require(cfg.bucket, "BUCKET")
    require(cfg.aws_account_id, "AWS_ACCOUNT_ID")
    require(cfg.polaris_root_client_secret, "POLARIS_ROOT_CLIENT_SECRET")

    wait_for_health(cfg.polaris_health_uri)
    token = root_token(cfg.polaris_uri, cfg.polaris_root_client_id, cfg.polaris_root_client_secret)
    mgmt = cfg.polaris_mgmt_uri

    print(f"Creating catalog {cfg.polaris_catalog} at {cfg.warehouse_url}")
    call("POST", f"{mgmt}/catalogs", token, {
        "catalog": {
            "name": cfg.polaris_catalog,
            "type": "INTERNAL",
            "properties": {"default-base-location": cfg.warehouse_url},
            "storageConfigInfo": {
                "storageType": "S3",
                "roleArn": f"arn:aws:iam::{cfg.aws_account_id}:role/nyc-taxi-polaris",
                "region": cfg.aws_region,
                "allowedLocations": [cfg.warehouse_url],
            },
        }
    })

    print(f"Creating principal {PRINCIPAL}")
    resp = call("POST", f"{mgmt}/principals", token, {"principal": {"name": PRINCIPAL}})
    if resp.status_code == 409:
        # Secret is only returned at creation time; rotate to obtain a fresh one.
        resp = call("POST", f"{mgmt}/principals/{PRINCIPAL}/rotate", token, ok_conflict=False)
    creds = resp.json()["credentials"]

    print(f"Creating roles {PRINCIPAL_ROLE} / {CATALOG_ROLE}")
    call("POST", f"{mgmt}/principal-roles", token, {"principalRole": {"name": PRINCIPAL_ROLE}})
    call("PUT", f"{mgmt}/principals/{PRINCIPAL}/principal-roles", token,
         {"principalRole": {"name": PRINCIPAL_ROLE}})
    call("POST", f"{mgmt}/catalogs/{cfg.polaris_catalog}/catalog-roles", token,
         {"catalogRole": {"name": CATALOG_ROLE}})
    call("PUT", f"{mgmt}/catalogs/{cfg.polaris_catalog}/catalog-roles/{CATALOG_ROLE}/grants", token,
         {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}})
    call("PUT", f"{mgmt}/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{cfg.polaris_catalog}", token,
         {"catalogRole": {"name": CATALOG_ROLE}})

    env_file = REPO_ROOT / ".polaris.env"
    env_file.write_text(
        "# Written by polaris_bootstrap.py. Gitignored. Do not commit.\n"
        f"POLARIS_CLIENT_ID={creds['clientId']}\n"
        f"POLARIS_CLIENT_SECRET={creds['clientSecret']}\n"
    )
    env_file.chmod(0o600)
    print(f"Wrote Spark principal credentials to {env_file.name}")


if __name__ == "__main__":
    main()
