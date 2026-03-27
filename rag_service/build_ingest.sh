#!/bin/bash

# stops script immediately on failure
set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=eu-west-2
REPO=c22-stocksiphon-ingest-lambda

# build ingest Docker image for AWS Lambda
docker buildx build --platform linux/amd64 --provenance=false -f dockerfile.ingest \
-t stocksiphon-rag-ingest:latest .

# create repo
aws ecr create-repository --repository-name $REPO --region $REGION

# authenticate Docker to ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# tag image
docker tag stocksiphon-rag-ingest:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO:latest

# push image to ECR
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO:latest