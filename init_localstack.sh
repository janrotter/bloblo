#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID="test"

export AWS_SECRET_ACCESS_KEY="test"

export AWS_DEFAULT_REGION="us-east-1"

aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket sample-bucket

aws --endpoint-url=http://localhost:4566 s3 cp testdockerimage/sha256:dfcff6d93b39097b3e4f343e505e1af69ccc98d4122439edc882f1ab908f48cb s3://sample-bucket/sha256:dfcff6d93b39097b3e4f343e505e1af69ccc98d4122439edc882f1ab908f48cb

aws --endpoint-url=http://localhost:4566 s3 ls s3://sample-bucket
