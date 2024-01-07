#!/bin/bash

# Set your S3 bucket name and path
S3_BUCKET="processed-tennis-stats-data"
S3_PATH="s3://$S3_BUCKET/"

# Set your Glue database name and crawler name
GLUE_DATABASE="processed-data-crawler-database"
GLUE_CRAWLER="processed-data-crawler"

# Create the Glue crawler
aws glue create-crawler \
    --region us-east-1 \
    --name $GLUE_CRAWLER \
    --role "arn:aws:iam::363710412655:role/LabRole" \
    --database-name $GLUE_DATABASE \
    --targets "S3Targets=[{Path='$S3_PATH'}]"
