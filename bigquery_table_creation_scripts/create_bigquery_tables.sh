#!/bin/bash

# Set dataset name
PROJECT_ID="financial-transactions-data"
DATASET_NAME="test"

# Create BigQuery Dataset
echo "Creating BigQuery dataset: $PROJECT_ID:$DATASET_NAME..."
bq mk --location=US $PROJECT_ID:$DATASET_NAME

# Create Tables
echo "Creating tables..."
bq mk --table $PROJECT_ID:$DATASET_NAME.test_card_data ./table_schemas/test_card_data.json
bq mk --table $PROJECT_ID:$DATASET_NAME.test_card_data_errors ./table_schemas/test_card_data_errors.json

echo "All tables created successfully!"