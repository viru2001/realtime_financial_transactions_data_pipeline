import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

class TokenizeAndMaskDoFn(beam.DoFn):
    def __init__(self, project_id, location_id, key_ring_id, crypto_key_id, dek_gcs_path):
        self.project_id = project_id
        self.location_id = location_id
        self.key_ring_id = key_ring_id
        self.crypto_key_id = crypto_key_id
        self.dek_gcs_path = dek_gcs_path
        self.kms_client = None
        self.fpe = None

    def setup(self):
        import pyffx
        from google.cloud import kms,storage

        # Initialize KMS client
        self.kms_client = kms.KeyManagementServiceClient()

        #1. Parse GCS path and Read encrypted DEK
        if not self.dek_gcs_path.startswith('gs://'):
            raise ValueError("DEK path must start with 'gs://'")
        path_parts = self.dek_gcs_path[5:].split('/', 1)
        if len(path_parts) != 2:
            raise ValueError("Invalid GCS path format")
        bucket_name, blob_name = path_parts

        # Initialize GCS client and download encrypted DEK
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        encrypted_dek = blob.download_as_bytes()

        # 2. Decrypt DEK via Cloud KMS
        key_name = self.kms_client.crypto_key_path(
            self.project_id, self.location_id, self.key_ring_id, self.crypto_key_id
        )
        response = self.kms_client.decrypt(
            request={'name': key_name, 'ciphertext': encrypted_dek}
        )
        dek = response.plaintext  # plaintext DEK

        self.fpe = pyffx.Integer(dek, length=16)

    

    def process(self, element_json):
        from datetime import datetime
        import apache_beam as beam
        import logging

        def unwrap_primitives(record):
            unwrapped = {}
            for k, v in record.items():
                # detect the { "string": "..."} / {"int": ...} pattern:
                if isinstance(v, dict) and len(v) == 1:
                    typ, prim = next(iter(v.items()))
                    # you can whitelist the primitive types you care about:
                    if typ in ("string", "int", "float"):
                        unwrapped[k] = prim
                        continue
                # otherwise, leave as‐is
                unwrapped[k] = v
            return unwrapped

        record = json.loads(element_json)
        record = unwrap_primitives(record)
        logging.info(record)
        pan = record.get('card_number')
        if not pan or len(pan) < 13 or len(pan) > 19:
            # raise ValueError(f"Invalid PAN: {pan}")
             # Write invalid records to the error table
            error_record = {
                'transaction_id': record.get('transaction_id'),
                'timestamp': datetime.now().isoformat(),
                'raw_message': element_json,
                'error': f"Invalid Card Number",
            }
            yield beam.pvalue.TaggedOutput('errors', error_record)
            return

        # Mask: first 6 & last 4
        masked = pan[:6] + '*' * 6 + pan[-4:]

        # Tokenize: FPE encrypt
        token = str(self.fpe.encrypt(int(pan))).zfill(16)

        # Update record
        record['masked_card_number'] = masked
        record['card_token'] = token
        record.pop('card_number', None)

        output_record = {
            "transaction_id": record.get("transaction_id"),
            "customer_id": record.get("customer_id"),
            "account_id": record.get("account_id"),
            "merchant_id": record.get("merchant_id"),
            "merchant_category_code_id": record.get("merchant_category_code_id"),
            "is_recurring": record.get("is_recurring"),
            "transaction_datetime": record.get("transaction_datetime"),
            "amount": record.get("amount"),
            "tax_amount": record.get("tax_amount"),
            "discount_amount": record.get("discount_amount"),
            "total_amount": record.get("total_amount"),
            "transaction_channel": record.get("transaction_channel"),
            "masked_card_number": record.get("masked_card_number"),
            "card_token": record.get("card_token"),
            "card_bin": record.get("card_bin"),
            "card_provider": record.get("card_provider"),
            "payment_gateway_id": record.get("payment_gateway_id"),
            "device_type_id": record.get("device_type_id"),
            "ip_address": record.get("ip_address"),
            "risk_score": record.get("risk_score")
        }

        yield output_record

def run():
    pub_sub_subscription = "projects/financial-transactions-data/subscriptions/fact_transactions_sub"
    project_id = "financial-transactions-data"
    dataset_name = "transactions_data"
    table_name = "fact_transactions"

    # Define KMS and DEK parameters
    location_id = "global"  
    key_ring_id = "card-tokenization-ring"
    crypto_key_id = "token-key"
    dek_gcs_path = "gs://card_token_keys/dek.enc"

    # Configure pipeline options for streaming execution
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        results= (
            p
            # Read raw messages from Pub/Sub as bytes, decode to UTF-8 strings
            | 'ReadPubSub' >> ReadFromPubSub(subscription=pub_sub_subscription).with_output_types(bytes)
            | 'DecodeUTF8' >> beam.Map(lambda b: b.decode('utf-8'))

            # Tokenize & mask card numbers
            | 'TokenizeAndMask' >> beam.ParDo(
                TokenizeAndMaskDoFn(
                    project_id=project_id,
                    location_id=location_id,
                    key_ring_id=key_ring_id,
                    crypto_key_id=crypto_key_id,
                    dek_gcs_path=dek_gcs_path
                )
            ).with_outputs('errors', main='valid_records')
        )
        # Write valid records to the main BigQuery table
        results.valid_records |  'WriteToBQ' >> WriteToBigQuery(
                table=f'{project_id}:{dataset_name}.{table_name}',
                schema={
                    "fields": [
                        {"name": "transaction_id", "type": "STRING"},
                        {"name": "customer_id", "type": "INTEGER"},
                        {"name": "account_id", "type": "INTEGER"},
                        {"name": "merchant_id", "type": "INTEGER"},
                        {"name": "merchant_category_code_id", "type": "INTEGER"},
                        {"name": "is_recurring", "type": "BOOLEAN"},
                        {"name": "transaction_datetime", "type": "STRING"},
                        {"name": "amount", "type": "FLOAT"},
                        {"name": "tax_amount", "type": "FLOAT"},
                        {"name": "discount_amount", "type": "FLOAT"},
                        {"name": "total_amount", "type": "FLOAT"},
                        {"name": "transaction_channel", "type": "STRING"},
                        {"name": "masked_card_number", "type": "STRING"},
                        {"name":"card_token", "type": "STRING"},
                        {"name": "card_bin", "type": "STRING"},
                        {"name": "card_provider", "type": "STRING"},
                        {"name": "payment_gateway_id", "type": "INTEGER"},
                        {"name": "device_type_id", "type": "INTEGER"},
                        {"name": "ip_address", "type": "STRING"},
                        {"name": "risk_score", "type": "FLOAT"}
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        
        # Write error records to the error BigQuery table
        results.errors | 'WriteErrorsToBQ' >> WriteToBigQuery(
            table=f'{project_id}:{dataset_name}.fact_transactions_errors',
            schema={
                'fields': [
                    {'name': 'transaction_id', 'type': 'STRING'},
                    {'name': 'timestamp', 'type': 'STRING'},
                    {'name': 'raw_message', 'type': 'STRING'},
                    {'name': 'error', 'type': 'STRING'},
                ]
            },
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
        )

if __name__ == '__main__':
    run()
