set -e

AWS_REGION="eu-west-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPO_NAME="c22-stocksiphon-alpaca-ecr"
IMAGE_TAG="latest"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}"

echo "Authenticating Docker with ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin \
    "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "Creating ECR repository (if it doesn't exist)..."
aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$AWS_REGION" 2>/dev/null \
  || aws ecr create-repository --repository-name "$REPO_NAME" --region "$AWS_REGION"

echo "Building Docker image..."
docker build --platform linux/amd64 -t "${REPO_NAME}:${IMAGE_TAG}" .

echo "Tagging image for ECR..."
docker tag "${REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"

echo "Pushing image to ECR..."
docker push "${ECR_URI}:${IMAGE_TAG}"

echo "Done! Image pushed to: ${ECR_URI}:${IMAGE_TAG}"
