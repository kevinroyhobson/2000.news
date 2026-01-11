#!/bin/bash
set -e

AWS_REGION="us-east-2"
CLOUDFRONT_DIST_ID="E1DGN8VLRTD5XC"
S3_BUCKET="www.2000.news"
PREVIOUS_DEPLOY_DIR=".previous-deploy"

TARGET="${1:-all}"

deploy_backend() {
    echo "=== Deploying backend ==="
    cd backend
    sam build
    sam deploy --stack-name news-2000 --region "$AWS_REGION"
    cd ..
}

deploy_frontend() {
    echo "=== Deploying frontend ==="
    cd frontend

    echo "Building React app..."
    npm run build

    echo "Backing up current deployment to $PREVIOUS_DEPLOY_DIR..."
    rm -rf "../$PREVIOUS_DEPLOY_DIR"
    mkdir -p "../$PREVIOUS_DEPLOY_DIR"
    aws s3 sync "s3://$S3_BUCKET" "../$PREVIOUS_DEPLOY_DIR" --region "$AWS_REGION" --quiet

    echo "Uploading new build to S3..."
    aws s3 sync build/ "s3://$S3_BUCKET" --delete --acl public-read --region "$AWS_REGION"

    echo "Invalidating CloudFront cache..."
    aws cloudfront create-invalidation --distribution-id "$CLOUDFRONT_DIST_ID" --paths "/*" --query 'Invalidation.Id' --output text

    cd ..

    echo ""
    echo "To roll back frontend:"
    echo "  aws s3 sync $PREVIOUS_DEPLOY_DIR s3://$S3_BUCKET --delete --acl public-read --region $AWS_REGION && aws cloudfront create-invalidation --distribution-id $CLOUDFRONT_DIST_ID --paths '/*'"
}

case "$TARGET" in
    backend)
        deploy_backend
        ;;
    frontend)
        deploy_frontend
        ;;
    all|"")
        deploy_backend
        deploy_frontend
        ;;
    *)
        echo "Usage: ./deploy.sh [backend|frontend|all]"
        exit 1
        ;;
esac

echo ""
echo "=== Deployment complete ==="
