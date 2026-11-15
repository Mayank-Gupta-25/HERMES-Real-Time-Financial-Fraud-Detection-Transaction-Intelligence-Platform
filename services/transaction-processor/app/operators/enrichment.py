"""
Customer & Merchant Context Enrichment Operator (MOD-02-FN-01).

Joins incoming cleaned `TransactionEvent` records with `customer:{customer_id}:profile`
and `merchant:{merchant_id}:profile` from Redis via `RedisProfileLookup` (`TC-10`).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..connectors.redis_lookup import RedisProfileLookup


class EnrichmentOperator:
    """
    Enriches cleaned transaction events with customer and merchant profiles from Redis.
    Falls back to safe defaults with `enrichment_hit = False` when a profile is not found.
    """

    def __init__(self, redis_lookup: Optional[RedisProfileLookup] = None) -> None:
        self.redis_lookup = redis_lookup or RedisProfileLookup()
        self.enriched_count: int = 0
        self.cache_or_redis_hits: int = 0
        self.profile_misses: int = 0

    def enrich(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Join customer and merchant context onto a validated `TransactionEvent`."""
        customer_id = str(event["customer_id"])
        merchant_id = str(event["merchant_id"])
        loc = event.get("location") or {}

        cust_profile = self.redis_lookup.get_customer_profile(customer_id)
        merch_profile = self.redis_lookup.get_merchant_profile(merchant_id)

        customer_hit = cust_profile is not None
        merchant_hit = merch_profile is not None
        enrichment_hit = customer_hit and merchant_hit

        if enrichment_hit:
            self.cache_or_redis_hits += 1
        else:
            self.profile_misses += 1

        enriched: Dict[str, Any] = dict(event)

        if cust_profile is not None:
            home_country = str(cust_profile.get("customer_home_country", "US")).upper()
            enriched["customer_risk_tier"] = str(cust_profile.get("customer_risk_tier", "low"))
            enriched["customer_home_country"] = home_country
            enriched["customer_home_city"] = str(cust_profile.get("customer_home_city", "Unknown"))
            enriched["customer_home_lat"] = float(cust_profile.get("customer_home_lat", 0.0))
            enriched["customer_home_lon"] = float(cust_profile.get("customer_home_lon", 0.0))
            enriched["customer_avg_amount"] = float(cust_profile.get("customer_avg_amount", 75.0))
            tx_country = str(loc.get("country", home_country)).upper()
            enriched["is_international"] = bool(event.get("is_international", False) or (tx_country != home_country))
        else:
            enriched["customer_risk_tier"] = "low"
            enriched["customer_home_country"] = str(loc.get("country", "US")).upper()
            enriched["customer_home_city"] = str(loc.get("city", "Unknown"))
            enriched["customer_home_lat"] = float(loc.get("latitude", 0.0))
            enriched["customer_home_lon"] = float(loc.get("longitude", 0.0))
            enriched["customer_avg_amount"] = 75.0

        if merch_profile is not None:
            enriched["merchant_name"] = str(merch_profile.get("merchant_name", f"Merchant-{merchant_id[:8]}"))
            enriched["merchant_is_high_risk"] = bool(merch_profile.get("merchant_is_high_risk", False))
            enriched["merchant_blacklisted"] = bool(merch_profile.get("merchant_blacklisted", False))
        else:
            enriched["merchant_name"] = f"Merchant-{merchant_id[:8]}"
            enriched["merchant_is_high_risk"] = False
            enriched["merchant_blacklisted"] = False

        enriched["customer_enrichment_hit"] = customer_hit
        enriched["merchant_enrichment_hit"] = merchant_hit
        enriched["enrichment_hit"] = enrichment_hit

        self.enriched_count += 1
        return enriched

    def enrich_batch(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.enrich(ev) for ev in events]
