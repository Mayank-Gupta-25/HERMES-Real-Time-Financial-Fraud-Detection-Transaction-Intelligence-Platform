"""
Date-Partitioned (`event_date=YYYY-MM-DD`) Snappy Parquet Archival Worker (`MOD-05-FN-04`).

Per `Lab678_21.pdf` Section 6 (`DIS-13`) and Blueprint `STEP AAKARSH-MOD05-01`:
  - Archives scored transaction records (`FraudDecision`) into Snappy-compressed
    Apache Parquet files under `transactions/event_date=YYYY-MM-DD/batch_<ts>_<uuid>.parquet`.
  - Partitions strictly by `event_date=YYYY-MM-DD` — NEVER by high-cardinality keys
    such as `customer_id` or `merchant_id` (preventing the small-file problem).
  - Supports both local filesystem archival (`archive_dir`) and MinIO S3 upload
    (`s3://hermes-archive/transactions/event_date=YYYY-MM-DD/...`) when a MinIO
    client is provided or configured.
  - Provides `read_archived_summary(start_date, end_date)` with partition pruning
    over `event_date=YYYY-MM-DD` directories using `pyarrow.parquet`.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Optional
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from minio import Minio  # type: ignore

    HAS_MINIO = True
except ImportError:
    Minio = None
    HAS_MINIO = False

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_LOCAL_ARCHIVE_DIR = REPO_ROOT / "data" / "archive"

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("event_time", pa.string()),
        pa.field("event_date", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("merchant_id", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("category", pa.string()),
        pa.field("country", pa.string()),
        pa.field("city", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("fraud_score", pa.float64()),
        pa.field("rule_score", pa.float64()),
        pa.field("ml_score", pa.float64()),
        pa.field("stat_score", pa.float64()),
        pa.field("risk_band", pa.string()),
        pa.field("decision", pa.string()),
        pa.field("triggered_rules_json", pa.string()),
        pa.field("feature_values_json", pa.string()),
        pa.field("provenance_json", pa.string()),
        pa.field("model_version", pa.string()),
        pa.field("rule_version", pa.string()),
        pa.field("schema_version", pa.string()),
    ]
)


def extract_event_date(event_time: Any) -> str:
    """Extract canonical `YYYY-MM-DD` date string from UTC `event_time`."""
    if isinstance(event_time, datetime):
        return event_time.astimezone(timezone.utc).strftime("%Y-%m-%d")
    if isinstance(event_time, str) and len(event_time) >= 10:
        candidate = event_time.strip()[:10]
        if len(candidate) == 10 and candidate[4] == "-" and candidate[7] == "-":
            return candidate
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _normalize_record_for_parquet(rec: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(rec.get("event_id") or rec.get("transaction_id") or uuid.uuid4())
    event_time = str(
        rec.get("event_time")
        or rec.get("timestamp")
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    event_date = extract_event_date(event_time)
    loc = rec.get("location") or {}

    triggered_rules = rec.get("triggered_rules")
    if isinstance(triggered_rules, str):
        triggered_rules_str = triggered_rules
    else:
        triggered_rules_str = json.dumps(list(triggered_rules or []))

    feature_values = rec.get("feature_values")
    if isinstance(feature_values, str):
        feature_values_str = feature_values
    else:
        feature_values_str = json.dumps(dict(feature_values or {}))

    provenance = rec.get("provenance")
    if isinstance(provenance, str):
        provenance_str = provenance
    else:
        provenance_str = json.dumps(dict(provenance or {}))

    return {
        "event_id": event_id,
        "event_time": event_time,
        "event_date": event_date,
        "customer_id": str(rec.get("customer_id", "")),
        "merchant_id": str(rec.get("merchant_id", "")),
        "amount": float(rec.get("amount", 0.0) or 0.0),
        "currency": str(rec.get("currency", "USD") or "USD"),
        "channel": str(rec.get("channel", "online") or "online"),
        "category": str(rec.get("category") or rec.get("merchant_category") or "unknown"),
        "country": str(loc.get("country") or rec.get("country") or "US"),
        "city": str(loc.get("city") or rec.get("city") or "Unknown"),
        "latitude": float(loc.get("latitude", rec.get("latitude", 0.0)) or 0.0),
        "longitude": float(loc.get("longitude", rec.get("longitude", 0.0)) or 0.0),
        "fraud_score": float(rec.get("fraud_score", 0.0) or 0.0),
        "rule_score": float(rec.get("rule_score", 0.0) or 0.0),
        "ml_score": float(rec.get("ml_score", 0.0) or 0.0),
        "stat_score": float(
            rec.get("stat_score", rec.get("z_score_signal", 0.0)) or 0.0
        ),
        "risk_band": str(rec.get("risk_band") or rec.get("risk_level") or "low"),
        "decision": str(rec.get("decision", "approve") or "approve"),
        "triggered_rules_json": triggered_rules_str,
        "feature_values_json": feature_values_str,
        "provenance_json": provenance_str,
        "model_version": str(rec.get("model_version", "ensemble-v1.0.0")),
        "rule_version": str(rec.get("rule_version", "rules-v1.0")),
        "schema_version": str(rec.get("schema_version", "1.0")),
    }


class ParquetArchiver:
    """
    Cold-tier Parquet archiver writing Snappy-compressed `.parquet` files partitioned
    strictly by `transactions/event_date=YYYY-MM-DD/batch_<ts>_<id>.parquet`.
    """

    def __init__(
        self,
        archive_dir: Optional[str | Path] = None,
        bucket_name: str = "hermes-archive",
        compression: str = "snappy",
        minio_client: Optional[Any] = None,
    ) -> None:
        self.archive_dir = Path(archive_dir) if archive_dir is not None else DEFAULT_LOCAL_ARCHIVE_DIR
        self.bucket_name = bucket_name
        self.compression = compression
        self.minio_client = minio_client
        self.files_written: List[str] = []
        self.rows_written: int = 0

    def archive_batch(self, records: Iterable[Dict[str, Any]]) -> List[str]:
        """
        Group `records` by `event_date=YYYY-MM-DD`, serialize each partition group
        as a Snappy-compressed Parquet file, and return the relative object keys
        (`transactions/event_date=YYYY-MM-DD/batch_<ts>_<uuid>.parquet`).
        """
        by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in records:
            norm = _normalize_record_for_parquet(rec)
            by_date[norm["event_date"]].append(norm)

        written_keys: List[str] = []
        ts_ms = int(time.time() * 1000)

        for event_date in sorted(by_date.keys()):
            partition_rows = by_date[event_date]
            batch_id = uuid.uuid4().hex[:8]
            rel_key = f"transactions/event_date={event_date}/batch_{ts_ms}_{batch_id}.parquet"

            table = pa.Table.from_pylist(partition_rows, schema=PARQUET_SCHEMA)
            local_path = self.archive_dir / rel_key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, local_path, compression=self.compression)

            if self.minio_client is not None and hasattr(self.minio_client, "put_object"):
                buf = io.BytesIO()
                pq.write_table(table, buf, compression=self.compression)
                data_bytes = buf.getvalue()
                self.minio_client.put_object(
                    self.bucket_name,
                    rel_key,
                    io.BytesIO(data_bytes),
                    length=len(data_bytes),
                    content_type="application/octet-stream",
                )

            written_keys.append(rel_key)
            self.files_written.append(rel_key)
            self.rows_written += len(partition_rows)

        return written_keys

    def write_batch(self, records: Iterable[Dict[str, Any]]) -> List[str]:
        """Alias for `archive_batch`."""
        return self.archive_batch(records)

    def read_archived_records(
        self,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """Read back archived Parquet rows using date-partition pruning over `[start_date, end_date]`."""
        tx_root = self.archive_dir / "transactions"
        if not tx_root.exists():
            return []

        rows: List[Dict[str, Any]] = []
        for part_dir in sorted(tx_root.iterdir()):
            if not part_dir.is_dir() or not part_dir.name.startswith("event_date="):
                continue
            part_date = part_dir.name.split("=", 1)[1]
            if not (start_date <= part_date <= end_date):
                continue
            for pq_file in sorted(part_dir.glob("*.parquet")):
                table = pq.read_table(pq_file)
                for item in table.to_pylist():
                    item["triggered_rules"] = json.loads(item.get("triggered_rules_json") or "[]")
                    item["feature_values"] = json.loads(item.get("feature_values_json") or "{}")
                    item["provenance"] = json.loads(item.get("provenance_json") or "{}")
                    rows.append(item)
        return rows

    def read_archived_summary(
        self,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Scan date-pruned `event_date=YYYY-MM-DD` Parquet partitions in `[start_date, end_date]`
        and return aggregated row-group and compliance summary metrics (`TC-23`).
        """
        tx_root = self.archive_dir / "transactions"
        partitions_scanned: List[str] = []
        files_scanned = 0
        total_rows = 0
        total_amount = 0.0
        fraud_count = 0
        flagged_count = 0
        blocked_count = 0
        score_sum = 0.0

        if tx_root.exists():
            for part_dir in sorted(tx_root.iterdir()):
                if not part_dir.is_dir() or not part_dir.name.startswith("event_date="):
                    continue
                part_date = part_dir.name.split("=", 1)[1]
                if not (start_date <= part_date <= end_date):
                    continue
                partitions_scanned.append(part_date)
                for pq_file in sorted(part_dir.glob("*.parquet")):
                    files_scanned += 1
                    table = pq.read_table(pq_file)
                    records = table.to_pylist()
                    total_rows += len(records)
                    for r in records:
                        total_amount += float(r.get("amount", 0.0) or 0.0)
                        score_sum += float(r.get("fraud_score", 0.0) or 0.0)
                        dec = str(r.get("decision", "approve"))
                        if dec in ("flag", "block"):
                            fraud_count += 1
                        if dec == "flag":
                            flagged_count += 1
                        elif dec == "block":
                            blocked_count += 1

        return {
            "bucket": self.bucket_name,
            "start_date": start_date,
            "end_date": end_date,
            "compression": self.compression,
            "partitions_scanned": partitions_scanned,
            "files_scanned": files_scanned,
            "total_rows": total_rows,
            "total_amount": round(total_amount, 2),
            "fraud_count": fraud_count,
            "flagged_count": flagged_count,
            "blocked_count": blocked_count,
            "avg_fraud_score": round(score_sum / total_rows, 4) if total_rows > 0 else 0.0,
        }
