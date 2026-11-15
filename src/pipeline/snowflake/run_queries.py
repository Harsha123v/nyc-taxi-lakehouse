"""Run snowflake/queries.sql and save each named result to results/<name>.csv.

Statements without a "-- name:" tag (USE ..., ALTER SESSION ...) are executed
but not saved. Credentials come from .env: key-pair auth via
SNOWFLAKE_PRIVATE_KEY_FILE is preferred; SNOWFLAKE_PASSWORD may hold a
password or a programmatic access token.

If you prefer, run queries.sql in Snowsight and download each result as CSV
into results/ instead.

Run: python -m pipeline.snowflake.run_queries
"""
from __future__ import annotations

import csv
import os
import re

from dotenv import load_dotenv

from pipeline.config import REPO_ROOT, RESULTS_DIR

NAME_RE = re.compile(r"--\s*name:\s*([A-Za-z0-9_]+)")


def split_statements(sql_text: str) -> list[tuple[str | None, str]]:
    """Return (name or None, statement) pairs.

    Works line by line so ';' inside '--' comments is ignored. A statement ends
    at a code line ending in ';'. Assumes no ';' inside string literals.
    """
    out: list[tuple[str | None, str]] = []
    name: str | None = None
    code_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            match = NAME_RE.match(stripped)
            if match:
                name = match.group(1)
            continue
        if not stripped:
            continue
        code_lines.append(line.rstrip())
        if stripped.endswith(";"):
            statement = "\n".join(code_lines).strip().rstrip(";").strip()
            out.append((name, statement))
            name, code_lines = None, []
    if code_lines:
        out.append((name, "\n".join(code_lines).strip()))
    return out


def connect():
    import snowflake.connector  # imported lazily so helpers are testable without the connector

    load_dotenv(REPO_ROOT / ".env")
    env = os.environ
    params = dict(
        account=env["SNOWFLAKE_ACCOUNT"],
        user=env["SNOWFLAKE_USER"],
        role=env.get("SNOWFLAKE_ROLE"),
        warehouse=env.get("SNOWFLAKE_WAREHOUSE"),
        database=env.get("SNOWFLAKE_DATABASE"),
        schema=env.get("SNOWFLAKE_SCHEMA"),
    )
    if env.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        params["private_key_file"] = env["SNOWFLAKE_PRIVATE_KEY_FILE"]
        params["authenticator"] = "SNOWFLAKE_JWT"
    else:
        params["password"] = env["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**params)


def main() -> None:
    statements = split_statements((REPO_ROOT / "snowflake" / "queries.sql").read_text())
    RESULTS_DIR.mkdir(exist_ok=True)
    with connect() as conn, conn.cursor() as cur:
        for name, stmt in statements:
            cur.execute(stmt)
            if not name:
                continue
            path = RESULTS_DIR / f"{name}.csv"
            with path.open("w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow([d[0].lower() for d in cur.description])
                rows = cur.fetchall()
                writer.writerows(rows)
            print(f"{name}: {len(rows)} rows -> {path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
