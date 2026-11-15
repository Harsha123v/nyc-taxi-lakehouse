"""Part 1: download one month of yellow-taxi trips + the zone lookup and land
them, byte-for-byte unchanged, in the raw layer of the private S3 bucket.

Idempotency:
  * Keys are deterministic (raw/yellow_trips/year=YYYY/month=MM/<file>), so a
    re-run writes to the same key and can never create a second copy.
  * Each object carries a sha256 of its content in S3 user metadata. If the
    object already exists with the same checksum, the upload is skipped.

Run: python -m pipeline.ingest.ingest_to_s3 --year 2025 --month 1
"""
from __future__ import annotations

import argparse
import hashlib
import tempfile
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

from pipeline.config import RAW_ZONE_KEY, TRIP_URL_TEMPLATE, ZONE_URL, load_config, raw_trip_key, require

CHUNK = 8 * 1024 * 1024


def download(url: str, dest: Path) -> str:
    """Stream `url` to `dest` and return the file's sha256 hex digest."""
    digest = hashlib.sha256()
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK):
                fh.write(chunk)
                digest.update(chunk)
    return digest.hexdigest()


def existing_checksum(s3, bucket: str, key: str) -> str | None:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return head.get("Metadata", {}).get("sha256")


def upload_if_changed(s3, bucket: str, key: str, path: Path, sha256: str, source_url: str) -> str:
    if existing_checksum(s3, bucket, key) == sha256:
        print(f"skip   s3://{bucket}/{key} (identical content already present)")
        return "skipped"
    s3.upload_file(
        str(path), bucket, key,
        ExtraArgs={
            "Metadata": {"sha256": sha256, "source-url": source_url},
            "ServerSideEncryption": "AES256",
        },
    )
    print(f"upload s3://{bucket}/{key} ({path.stat().st_size / 1e6:.1f} MB)")
    return "uploaded"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    cfg = load_config()
    bucket = require(cfg.bucket, "BUCKET")
    s3 = boto3.client("s3", region_name=cfg.aws_region)

    jobs = [
        (TRIP_URL_TEMPLATE.format(year=args.year, month=args.month), raw_trip_key(args.year, args.month)),
        (ZONE_URL, RAW_ZONE_KEY),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        for url, key in jobs:
            local = Path(tmp) / Path(key).name
            print(f"download {url}")
            sha = download(url, local)
            upload_if_changed(s3, bucket, key, local, sha, url)


if __name__ == "__main__":
    main()
