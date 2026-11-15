{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListWarehouseOnly",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET}",
      "Condition": { "StringLike": { "s3:prefix": ["warehouse/*", "warehouse"] } }
    },
    {
      "Sid": "BucketLocation",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::${BUCKET}"
    },
    {
      "Sid": "ReadOnlyIcebergFiles",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::${BUCKET}/warehouse/*"
    }
  ]
}
