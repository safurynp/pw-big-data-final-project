#!/bin/bash
# Run this script from this directory.

# Directory containing the CSV files
dir="../tennis_wta"

# Name of your S3 bucket
bucket="tennis-stats-data"

# Array of file patterns to match
file_patterns=("wta_matches_[0-9][0-9][0-9][0-9].csv" "wta_matches_qual_itf_[0-9][0-9][0-9][0-9].csv" "wta_rankings_*.csv" "wta_players.csv")

# Loop over all file patterns
for pattern in "${file_patterns[@]}"
do
  # Loop over all files matching the current pattern
  for file in $dir/$pattern
  do
    # Upload the file to the S3 bucket
    aws s3 cp $file s3://$bucket/
  done
done