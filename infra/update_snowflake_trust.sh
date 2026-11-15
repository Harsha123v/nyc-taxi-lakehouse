#!/usr/bin/env bash
# Finalize the trust policy of nyc-taxi-snowflake-read so that ONLY your
# Snowflake account's IAM user, presenting your external volume's external ID,
# can assume it. Values come from `DESC EXTERNAL VOLUME NYC_TAXI_VOL;`
# (STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID).
#
#   ADMIN_PROFILE=my-admin bash infra/update_snowflake_trust.sh <iam_user_arn> <external_id>
set -euo pipefail
cd "$(dirname "$0")/.."

SNOWFLAKE_IAM_USER_ARN="${1:?usage: update_snowflake_trust.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>}"
SNOWFLAKE_EXTERNAL_ID="${2:?missing STORAGE_AWS_EXTERNAL_ID}"
ADMIN_PROFILE="${ADMIN_PROFILE:-default}"

mkdir -p infra/rendered
tpl="$(cat infra/policies/snowflake-role-trust.json.tpl)"
tpl="${tpl//\$\{SNOWFLAKE_IAM_USER_ARN\}/$SNOWFLAKE_IAM_USER_ARN}"
tpl="${tpl//\$\{SNOWFLAKE_EXTERNAL_ID\}/$SNOWFLAKE_EXTERNAL_ID}"
printf '%s\n' "$tpl" > infra/rendered/snowflake-role-trust.json

aws --profile "$ADMIN_PROFILE" iam update-assume-role-policy \
  --role-name nyc-taxi-snowflake-read \
  --policy-document file://infra/rendered/snowflake-role-trust.json
echo "Trust policy updated. In Snowflake run: SELECT SYSTEM\$VERIFY_EXTERNAL_VOLUME('NYC_TAXI_VOL');"
