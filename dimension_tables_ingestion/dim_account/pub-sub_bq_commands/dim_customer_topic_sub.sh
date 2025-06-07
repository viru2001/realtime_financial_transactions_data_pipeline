gcloud config set project financial-transactions-data

# create pub/sub schema
gcloud pubsub schemas create dim_account \
    --type=avro \
    --definition-file=dim_account.avsc

# create topic

gcloud pubsub topics create dim_account \
    --schema=dim_account \
    --message-encoding=JSON

# create sub
gcloud pubsub subscriptions create dim_account_sub \
    --topic=dim_account \
    --bigquery-table=financial-transactions-data:transactions_data.dim_account \
    --use-topic-schema
