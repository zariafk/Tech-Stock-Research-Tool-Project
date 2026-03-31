#!/bin/bash
set -e

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="eu-west-2"
REPO_NAME=c22-stocksiphon-dashboard-ecr
TAG="latest"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"

echo "Authenticating Docker with ECR..."
aws ecr get-login-password --region "$REGION" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Building image..."
docker build --no-cache --platform linux/amd64 --provenance=false -t "${REPO_NAME}:${TAG}" .

echo "Tagging image..."
docker tag "${REPO_NAME}:${TAG}" "${ECR_URI}:${TAG}"

echo "Pushing to ECR..."
docker push "${ECR_URI}:${TAG}"

echo "Done! Image pushed to: ${ECR_URI}:${TAG}"
