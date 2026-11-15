from __future__ import annotations

from typing import Any

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None  # type: ignore

from .postgres import get_seed_dataset


class TimescalePool:
    """
    TimescaleDB time-series query adapter (transaction_metrics, cagg_tx_metrics_5m, cagg_tx_metrics_1h)
    with deterministic seeded bucket fallback.
    """

    def __init__(self) -> None:
        self._pool: Any = None

    @property
    def dataset(self) -> dict[str, Any]:
        return get_seed_dataset()

    async def get_volume_series(self, interval: str = "5m", range_str: str = "24h") -> list[dict[str, Any]]:
        buckets = list(self.dataset["timeseries_buckets"])
        step = 12 if interval == "1h" else (3 if interval == "15m" else 1)

        series: list[dict[str, Any]] = []
        for idx in range(0, len(buckets), step):
            chunk = buckets[idx : idx + step]
            if not chunk:
                continue
            tx_cnt = sum(int(b["tx_count"]) for b in chunk)
            tot_amt = round(sum(float(b["total_amount"]) for b in chunk), 2)
            fraud_cnt = sum(int(b["fraud_count"]) for b in chunk)
            avg_tps = round(sum(float(b["tps"]) for b in chunk) / len(chunk), 2)
            series.append(
                {
                    "bucket_time": chunk[0]["bucket_time"],
                    "tx_count": tx_cnt,
                    "tps": avg_tps,
                    "total_amount": tot_amt,
                    "avg_amount": round(tot_amt / max(tx_cnt, 1), 2),
                    "fraud_count": fraud_cnt,
                    "fraud_rate": round(fraud_cnt / max(tx_cnt, 1), 4),
                }
            )
        return series

    async def get_fraud_rate_series(self, interval: str = "5m", range_str: str = "24h") -> dict[str, Any]:
        vol_series = await self.get_volume_series(interval=interval, range_str=range_str)
        txs = self.dataset["transactions"]

        band_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for t in txs:
            rb = t.get("risk_band", "low")
            band_counts[rb] = band_counts.get(rb, 0) + 1

        timeline = [
            {
                "bucket_time": pt["bucket_time"],
                "tx_count": pt["tx_count"],
                "fraud_count": pt["fraud_count"],
                "fraud_rate": pt["fraud_rate"],
            }
            for pt in vol_series
        ]

        return {
            "series": timeline,
            "risk_band_breakdown": [
                {
                    "risk_band": band,
                    "count": cnt,
                    "percentage": round((cnt / max(len(txs), 1)) * 100.0, 2),
                }
                for band, cnt in band_counts.items()
            ],
        }


timescale_db = TimescalePool()
