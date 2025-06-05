#!/bin/bash

# Set dataset name
PROJECT_ID="financial-transactions-data"
DATASET_NAME="transactions_data"
# Create Tables
echo "Creating tables..."
bq mk --table $PROJECT_ID:$DATASET_NAME.dim_device_type dim_device_type.json
bq mk --table $PROJECT_ID:$DATASET_NAME.dim_mcc_codes dim_mcc_codes.json
bq mk --table $PROJECT_ID:$DATASET_NAME.dim_merchants dim_merchants.json
bq mk --table $PROJECT_ID:$DATASET_NAME.dim_payment_gateway dim_payment_gateway.json


echo "All tables created successfully!"