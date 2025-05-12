# Create a key ring

gcloud kms keyrings create card-tokenization-ring \
    --location global

# Create a symmetric encryption key 

gcloud kms keys create token-key \
    --location global \
    --keyring card-tokenization-ring \
    --purpose encryption \
    --rotation-period 90d \
    --next-rotation-time=2025-06-08T13:00:00Z


# commands to check whether key and keyrings created properly

gcloud kms keyrings list --location=global
gcloud kms keys list --keyring=card-tokenization-ring --location=global

# check versions of keys present in key-ring
gcloud kms keys versions list --key=token-key --keyring=card-tokenization-ring --location=global

# Generate a Random DEK Locally
import os
dek = os.urandom(32)  # 256-bit random key
with open("dek.bin", "wb") as f:
    f.write(dek)

# Wrap (Encrypt) the DEK with the KEK
gcloud kms encrypt \
  --location global \
  --keyring card-tokenization-ring \
  --key token-key \
  --plaintext-file dek.bin \
  --ciphertext-file dek.enc






# destroy specific version of key
gcloud kms keys versions destroy 1 \
    --key=card-token-key \
    --keyring=card-tokenization-ring \
    --location=global

# see current status of specific version of key
gcloud kms keys versions describe 1 \
  --key=card-token-key \
  --keyring=card-tokenization-ring \
  --location=global



