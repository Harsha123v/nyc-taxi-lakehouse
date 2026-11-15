#!/usr/bin/env bash
# One-time AWS setup. Run with an ADMIN profile that is an IAM user/role with
# admin rights (NOT the account root user):
#
#   ADMIN_PROFILE=my-admin bash infra/setup_aws.sh
#
# Creates:
#   - private, encrypted S3 bucket ${BUCKET} (public access fully blocked)
#   - IAM user  nyc-taxi-pipeline     (least privilege: raw/ + warehouse/ only)
#   - IAM role  nyc-taxi-polaris      (assumed by local Polaris, warehouse/ only)
#   - IAM role  nyc-taxi-snowflake-read (read-only warehouse/, trust finalized later)
#
# It does NOT write access keys anywhere in the repo. It prints instructions
# for creating a key and storing it in your local AWS CLI profile.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; source .env; set +a

: "${BUCKET:?BUCKET missing in .env}"
: "${AWS_REGION:?AWS_REGION missing in .env}"
ADMIN_PROFILE="${ADMIN_PROFILE:-default}"
AWS="aws --profile ${ADMIN_PROFILE} --region ${AWS_REGION}"

AWS_ACCOUNT_ID="$($AWS sts get-caller-identity --query Account --output text)"
CALLER_ARN="$($AWS sts get-caller-identity --query Arn --output text)"
if [[ "$CALLER_ARN" == *":root" ]]; then
  echo "Refusing to run as the account root user. Use an admin IAM user or role." >&2
  exit 1
fi
echo "Account ${AWS_ACCOUNT_ID}, caller ${CALLER_ARN}"

RENDER_DIR="infra/rendered"
mkdir -p "$RENDER_DIR"
render() {  # render <template> <output> [extra VAR=value ...]
  local tpl="$1" out="$2"; shift 2
  local content
  content="$(cat "$tpl")"
  content="${content//\$\{BUCKET\}/$BUCKET}"
  content="${content//\$\{AWS_ACCOUNT_ID\}/$AWS_ACCOUNT_ID}"
  for kv in "$@"; do
    local k="${kv%%=*}" v="${kv#*=}"
    content="${content//\$\{$k\}/$v}"
  done
  printf '%s\n' "$content" > "$out"
}

# ---------- S3 bucket ----------
if $AWS s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Bucket ${BUCKET} already exists, skipping create"
else
  if [[ "$AWS_REGION" == "us-east-1" ]]; then
    $AWS s3api create-bucket --bucket "$BUCKET"
  else
    $AWS s3api create-bucket --bucket "$BUCKET" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}"
  fi
  echo "Created bucket ${BUCKET}"
fi
$AWS s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
$AWS s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
$AWS s3api put-bucket-ownership-controls --bucket "$BUCKET" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'

# ---------- Pipeline IAM user ----------
render infra/policies/pipeline-user-policy.json.tpl "$RENDER_DIR/pipeline-user-policy.json"
if ! $AWS iam get-user --user-name nyc-taxi-pipeline >/dev/null 2>&1; then
  $AWS iam create-user --user-name nyc-taxi-pipeline >/dev/null
  echo "Created IAM user nyc-taxi-pipeline"
fi
$AWS iam put-user-policy --user-name nyc-taxi-pipeline \
  --policy-name nyc-taxi-pipeline-least-privilege \
  --policy-document "file://$RENDER_DIR/pipeline-user-policy.json"

# IAM is eventually consistent: a brand-new user may not be usable in a trust policy yet.
sleep 10

# ---------- Polaris role ----------
render infra/policies/polaris-role-trust.json.tpl  "$RENDER_DIR/polaris-role-trust.json"
render infra/policies/polaris-role-policy.json.tpl "$RENDER_DIR/polaris-role-policy.json"
if ! $AWS iam get-role --role-name nyc-taxi-polaris >/dev/null 2>&1; then
  $AWS iam create-role --role-name nyc-taxi-polaris \
    --assume-role-policy-document "file://$RENDER_DIR/polaris-role-trust.json" >/dev/null
  echo "Created IAM role nyc-taxi-polaris"
else
  $AWS iam update-assume-role-policy --role-name nyc-taxi-polaris \
    --policy-document "file://$RENDER_DIR/polaris-role-trust.json"
fi
$AWS iam put-role-policy --role-name nyc-taxi-polaris --policy-name warehouse-rw \
  --policy-document "file://$RENDER_DIR/polaris-role-policy.json"

# ---------- Snowflake read role (trust is a placeholder until the external volume exists) ----------
render infra/policies/snowflake-role-trust.json.tpl "$RENDER_DIR/snowflake-role-trust-placeholder.json" \
  "SNOWFLAKE_IAM_USER_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:root" \
  "SNOWFLAKE_EXTERNAL_ID=pending-snowflake-setup"
render infra/policies/snowflake-read-policy.json.tpl "$RENDER_DIR/snowflake-read-policy.json"
if ! $AWS iam get-role --role-name nyc-taxi-snowflake-read >/dev/null 2>&1; then
  $AWS iam create-role --role-name nyc-taxi-snowflake-read \
    --assume-role-policy-document "file://$RENDER_DIR/snowflake-role-trust-placeholder.json" >/dev/null
  echo "Created IAM role nyc-taxi-snowflake-read (trust finalized in Part 4)"
fi
$AWS iam put-role-policy --role-name nyc-taxi-snowflake-read --policy-name warehouse-read-only \
  --policy-document "file://$RENDER_DIR/snowflake-read-policy.json"

cat <<EOF

Done. Next, create an access key for the pipeline user and store it ONLY in
your local AWS CLI config (never in the repo):

  aws --profile ${ADMIN_PROFILE} iam create-access-key --user-name nyc-taxi-pipeline
  aws configure --profile nyc-taxi-pipeline     # paste the key + secret, region ${AWS_REGION}

Polaris role ARN:   arn:aws:iam::${AWS_ACCOUNT_ID}:role/nyc-taxi-polaris
Snowflake role ARN: arn:aws:iam::${AWS_ACCOUNT_ID}:role/nyc-taxi-snowflake-read
Set AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} in .env if you haven't.
EOF
