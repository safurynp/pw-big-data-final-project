#!/bin/bash

# Define your bucket
bucket="s3://tennis-stats-data"

# Move ATP matches CSV files to "matches" directory
aws s3 mv $bucket/ $bucket/atp_matches --recursive --exclude "*" --include "atp_matches*.csv"

# Move WTA matches CSV files to "matches" directory
aws s3 mv $bucket/ $bucket/wta_matches --recursive --exclude "*" --include "wta_matches*.csv"

# Move ATP players CSV files to "players" directory
aws s3 mv $bucket/ $bucket/atp_players --recursive --exclude "*" --include "atp_players*.csv"

# Move WTA players CSV files to "players" directory
aws s3 mv $bucket/ $bucket/wta_players --recursive --exclude "*" --include "wta_players*.csv"

# Move ATP rankings CSV files to "rankings" directory
aws s3 mv $bucket/ $bucket/atp_rankings --recursive --exclude "*" --include "atp_rankings*.csv"

# Move WTA rankings CSV files to "rankings" directory
aws s3 mv $bucket/ $bucket/wta_rankings --recursive --exclude "*" --include "wta_rankings*.csv"