#!/bin/bash

# Directory containing the CSV files
dir="/path/to/your/csv/files"

# Name of your S3 bucket
bucket="bucket-name"

# Loop over all CSV files in the directory
for file in $dir/*.csv
do
  # Upload the file to the S3 bucket
  aws s3 cp $file s3://$bucket/
done

# 