#!/bin/bash
set -e

echo "=== Backing up current S3 content to prior_deploy/ ==="
rm -rf prior_deploy
aws s3 sync s3://www.2000.news prior_deploy/

echo "=== Building production React app ==="
npm run build

echo "=== Syncing to S3 bucket www.2000.news ==="
aws s3 sync build/ s3://www.2000.news --delete --acl public-read

echo "=== Invalidating CloudFront distribution ==="
aws cloudfront create-invalidation --distribution-id E1DGN8VLRTD5XC --paths "/*"

echo "=== Deploy complete ==="
echo "Previous version backed up to prior_deploy/ (use 'aws s3 sync prior_deploy/ s3://www.2000.news' to revert)"
