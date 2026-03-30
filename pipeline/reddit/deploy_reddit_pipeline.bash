#!/bin/bash

set -e
# change placeholders to your values
AWS_ACCOUNT_ID=<account-id>
REGION="eu-west-2"
REPO_NAME=<ecr-repo-name>
TAG="latest"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"

echo "(Summary): Logging into ECR..."
aws ecr get-login-password --region "$REGION" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Building image..."
docker build --platform linux/amd64 --provenance=false -t "${REPO_NAME}:${TAG}" .

echo "Tagging image..."
docker tag "${REPO_NAME}:${TAG}" "${ECR_URI}:${TAG}"

echo "Pushing to ECR..."
docker push "${ECR_URI}:${TAG}"

echo "Done."