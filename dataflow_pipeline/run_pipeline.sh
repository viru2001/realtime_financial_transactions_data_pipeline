python transactions_pipeline_kms.py \
  --runner=DataflowRunner \
  --project=financial-transactions-data \
  --region=us-central1 \
  --temp_location=gs://transactions_data_dataflow_pipeline/temp \
  --staging_location=gs://transactions_data_dataflow_pipeline/staging \
  --job_name=financial-transactions-data-load \
  --service_account_email=financial-transactions-data-da@financial-transactions-data.iam.gserviceaccount.com \
  --requirements_file=requirements.txt