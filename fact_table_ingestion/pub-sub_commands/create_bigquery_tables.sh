# Set dataset name
PROJECT_ID="financial-transactions-data"
DATASET_NAME="transactions_data"

# Create BigQuery Dataset
bq mk --location=US $PROJECT_ID:$DATASET_NAME

# create bigquery table
bq mk --table $PROJECT_ID:$DATASET_NAME.fact_transactions ./fact_transactions.json