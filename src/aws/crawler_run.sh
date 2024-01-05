#!/bin/bash

# Set your Glue database name and crawler name
GLUE_DATABASE="raw-data-crawler-database"
GLUE_CRAWLER="raw-data-crawler"

# Delete existing Glue database (if exists)
aws glue delete-database --name $GLUE_DATABASE

# Start the Glue crawler
aws glue start-crawler --name $GLUE_CRAWLER --region us-east-1