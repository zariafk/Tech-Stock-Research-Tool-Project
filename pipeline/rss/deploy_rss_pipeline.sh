#!/bin/bash
set -e

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="eu-west-2"
<<<<<<< HEAD:pipeline/rss/deploy_rss_pipeline.sh
REPO_NAME="c22-stocksiphon-rss-ecr"
=======
REPO_NAME=c22-stocksiphon-reddit-ecr
>>>>>>> 33e391be5b041f869156e243fc032a5a58d489d5:pipeline/reddit/deploy_reddit_pipeline.bash
TAG="latest"

ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"

echo "Authenticating Docker with ECR..."
aws ecr get-login-password --region "$REGION" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Building image..."
docker build --platform linux/amd64 --provenance=false -t "${REPO_NAME}:${TAG}" .

echo "Tagging image..."
docker tag "${REPO_NAME}:${TAG}" "${ECR_URI}:${TAG}"

echo "Pushing to ECR..."
docker push "${ECR_URI}:${TAG}"

echo "Done! Image pushed to: ${ECR_URI}:${TAG}"
