#!/bin/bash

# stops script immediately on failure
set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=eu-west-2
REPO=c22-stocksiphon-query-lambda

# build query Docker image for AWS Lambda
docker buildx build --platform linux/amd64 --provenance=false -f dockerfile.query \
-t stocksiphon-rag-query:latest .

# create repo (or updates latest tag if repo already exists)
aws ecr create-repository --repository-name $REPO --region $REGION || true

# authenticate Docker to ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# tag image
docker tag stocksiphon-rag-query:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO:latest

# push image to ECR
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO:latest