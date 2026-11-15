#!/usr/bin/env bash
# HERMES MinIO Cold Archive & Model Bucket Initialization (`MOD-05-FN-04`)
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-hermes_minio}"
MINIO_ROOT_PASS="${MINIO_ROOT_PASS:-hermes_minio_pass}"
ARCHIVE_BUCKET="${MINIO_BUCKET:-hermes-archive}"
MODELS_BUCKET="${MINIO_MODELS_BUCKET:-hermes-models}"

echo "[minio-init] Waiting for MinIO at ${MINIO_ENDPOINT}..."
until mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASS}" >/dev/null 2>&1; do
  sleep 2
done

echo "[minio-init] Creating buckets ${ARCHIVE_BUCKET} and ${MODELS_BUCKET}..."
mc mb --ignore-existing "local/${ARCHIVE_BUCKET}"
mc mb --ignore-existing "local/${MODELS_BUCKET}"

# Configure 365-day lifecycle expiration rule on cold transaction archives
cat <<'EOF' > /tmp/lifecycle.json
{
  "Rules": [
    {
      "ID": "hermes-cold-archive-365d-retention",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "transactions/"
      },
      "Expiration": {
        "Days": 365
      }
    }
  ]
}
EOF

mc ilm import "local/${ARCHIVE_BUCKET}" < /tmp/lifecycle.json || true
echo "[minio-init] MinIO buckets and lifecycle policies initialized."
