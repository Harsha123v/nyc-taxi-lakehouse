#!/usr/bin/env bash
# Delete EVERYTHING this project created in AWS. Irreversible.
# Run only after the review is finished.
#
#   ADMIN_PROFILE=my-admin bash infra/teardown_aws.sh
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; source .env; set +a
ADMIN_PROFILE="${ADMIN_PROFILE:-default}"
AWS="aws --profile ${ADMIN_PROFILE} --region ${AWS_REGION}"

read -r -p "This permanently deletes s3://${BUCKET}, its contents, and the project IAM user/roles. Type the bucket name to confirm: " answer
[[ "$answer" == "$BUCKET" ]] || { echo "Aborted."; exit 1; }

# S3: objects then bucket
$AWS s3 rm "s3://${BUCKET}" --recursive || true
$AWS s3api delete-bucket --bucket "$BUCKET" || true

# IAM user: keys, inline policy, user
for key in $($AWS iam list-access-keys --user-name nyc-taxi-pipeline --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
  $AWS iam delete-access-key --user-name nyc-taxi-pipeline --access-key-id "$key"
done
$AWS iam delete-user-policy --user-name nyc-taxi-pipeline --policy-name nyc-taxi-pipeline-least-privilege 2>/dev/null || true
$AWS iam delete-user --user-name nyc-taxi-pipeline 2>/dev/null || true

# IAM roles
$AWS iam delete-role-policy --role-name nyc-taxi-polaris --policy-name warehouse-rw 2>/dev/null || true
$AWS iam delete-role --role-name nyc-taxi-polaris 2>/dev/null || true
$AWS iam delete-role-policy --role-name nyc-taxi-snowflake-read --policy-name warehouse-read-only 2>/dev/null || true
$AWS iam delete-role --role-name nyc-taxi-snowflake-read 2>/dev/null || true

rm -rf infra/rendered
echo "AWS resources deleted. Also run snowflake/teardown.sql in Snowflake and:"
echo "  docker compose down -v   # stop Polaris"
echo "  aws configure --profile nyc-taxi-pipeline   # or delete that profile from ~/.aws/credentials"
