gcloud config set project financial-transactions-data

# create pub/sub schema
gcloud pubsub schemas create dim_customer \
    --type=avro \
    --definition-file=dim_customer.avsc

# create topic

gcloud pubsub topics create dim_customer \
    --schema=dim_customer \
    --message-encoding=JSON

# create sub
gcloud pubsub subscriptions create dim_customer_sub \
    --topic=dim_customer \
    --bigquery-table=financial-transactions-data:transactions_data.dim_customer \
    --use-topic-schema
