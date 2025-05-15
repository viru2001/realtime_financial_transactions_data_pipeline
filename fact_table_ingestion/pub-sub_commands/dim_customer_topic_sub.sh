gcloud config set project financial-transactions-data

# create pub/sub schema
gcloud pubsub schemas create fact_transactions \
    --type=avro \
    --definition-file=fact_transactions.avsc

# create topic

gcloud pubsub topics create fact_transactions \
    --schema=fact_transactions \
    --message-encoding=JSON

# create sub

gcloud pubsub subscriptions create fact_transactions_sub \
    --topic=fact_transactions \
    --enable-exactly-once-delivery
