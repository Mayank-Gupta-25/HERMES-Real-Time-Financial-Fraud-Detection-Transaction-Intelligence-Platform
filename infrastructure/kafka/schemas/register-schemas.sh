#!/usr/bin/env bash
set -euo pipefail

SCHEMA_REGISTRY_URL="${KAFKA_SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SCHEMAS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL}..."
for i in {1..30}; do
  if curl -s -f "${SCHEMA_REGISTRY_URL}/subjects" > /dev/null 2>&1; then
    echo "Schema Registry is ready."
    break
  fi
  sleep 2
done

register_schema() {
  local subject="$1"
  local schema_file="$2"

  echo "Setting compatibility BACKWARD for subject ${subject}..."
  curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "BACKWARD"}' \
    "${SCHEMA_REGISTRY_URL}/config/${subject}" || true

  echo "Registering ${schema_file} under subject ${subject}..."
  python3 -c '
import json, sys, urllib.request
subject, path, registry = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path, "r", encoding="utf-8") as f:
    schema_str = f.read()
payload = json.dumps({"schema": schema_str}).encode("utf-8")
req = urllib.request.Request(
    f"{registry}/subjects/{subject}/versions",
    data=payload,
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    method="POST",
)
with urllib.request.urlopen(req) as resp:
    print(f"Registered {subject}: {resp.read().decode()}")
' "${subject}" "${schema_file}" "${SCHEMA_REGISTRY_URL}"
}

register_schema "hermes.transactions.raw-value" "${SCHEMAS_DIR}/transaction_event_v1.avsc"
register_schema "hermes.transactions.scored-value" "${SCHEMAS_DIR}/fraud_decision_v1.avsc"

echo "All Avro schemas registered successfully."
