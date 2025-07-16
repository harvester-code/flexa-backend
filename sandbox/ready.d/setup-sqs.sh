#!/bin/bash

echo "📥 [INIT] Creating SQS queues and DLQ..."

# 1) Dead Letter Queue 생성
DLQ_URL=$(awslocal sqs create-queue --queue-name my-queue-dlq --query 'QueueUrl' --output text)

# DLQ의 ARN 얻기
DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-name QueueArn \
    --query 'Attributes.QueueArn' --output text)

echo "✅ DLQ created: $DLQ_URL (ARN: $DLQ_ARN)"

# 2) 메인 큐 생성 시 RedrivePolicy 설정
awslocal sqs create-queue \
  --queue-name my-queue \
  --attributes RedrivePolicy='{"deadLetterTargetArn":"'"$DLQ_ARN"'","maxReceiveCount":5}'

echo "✅ SQS queue created: my-queue (with DLQ: my-queue-dlq)"
