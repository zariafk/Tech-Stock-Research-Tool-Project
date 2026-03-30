#!/bin/bash

set -e
# Load from environment variables (set in CI/CD or export before running)
AWS_ACCOUNT_ID="129033205317"
REGION="eu-west-2"
REPO_NAME="c22-stocksiphon-rss-ecr"
TAG="latest"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"

aws ecr get-login-password --region "$REGION" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Building image: ${REPO_NAME}:${TAG}"
docker build --platform linux/amd64 --provenance=false -t "${REPO_NAME}:${TAG}" .

echo "Tagging image: ${ECR_URI}:${TAG}"
docker tag "${REPO_NAME}:${TAG}" "${ECR_URI}:${TAG}"

echo "Pushing to ECR..."
docker push "${ECR_URI}:${TAG}"

echo "Deployment complete: ${ECR_URI}:${TAG}"