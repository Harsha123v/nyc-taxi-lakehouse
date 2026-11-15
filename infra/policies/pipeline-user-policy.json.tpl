{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyPipelinePrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET}",
      "Condition": {
        "StringLike": { "s3:prefix": ["raw/*", "warehouse/*", "raw", "warehouse"] }
      }
    },
    {
      "Sid": "BucketLocation",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::${BUCKET}"
    },
    {
      "Sid": "ReadWriteRawLayer",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::${BUCKET}/raw/*"
    },
    {
      "Sid": "ReadWriteIcebergWarehouse",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${BUCKET}/warehouse/*"
    },
    {
      "Sid": "LetLocalPolarisAssumeItsRole",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/nyc-taxi-polaris"
    }
  ]
}
