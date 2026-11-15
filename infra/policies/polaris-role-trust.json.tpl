{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "OnlyThePipelineUserMayAssume",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:user/nyc-taxi-pipeline" },
      "Action": "sts:AssumeRole"
    }
  ]
}
