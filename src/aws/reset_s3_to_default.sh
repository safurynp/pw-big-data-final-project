#!/bin/bash

# Define your bucket
bucket="s3://tennis-stats-data"

aws s3 mv $bucket/matches/ $bucket/ --recursive --include "*.cvs"
aws s3 mv $bucket/players/ $bucket/ --recursive --include "*.csv"
aws s3 mv $bucket/rankings/ $bucket/ --recursive --include "*.csv"
