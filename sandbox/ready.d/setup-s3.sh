#!/bin/bash

# export AWS_ACCESS_KEY_ID=000000000000 AWS_SECRET_ACCESS_KEY=000000000000

echo "🪣 [INIT] Creating S3 bucket..."

awslocal s3 mb s3://my-bucket

echo "✅ S3 bucket created: my-bucket"

