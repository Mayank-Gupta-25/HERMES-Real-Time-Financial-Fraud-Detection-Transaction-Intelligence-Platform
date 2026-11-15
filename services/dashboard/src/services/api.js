const API_BASE_URL =
  (typeof import.meta !== "undefined" &&
    import.meta.env &&
    import.meta.env.VITE_ANALYTICS_API_URL) ||
  "http://localhost:8002/api/v1/analytics";

export const MOCK_FIXTURES = {
  overview: {
    current_tps: 0.72,
    total_transactions: 500,
    total_volume_usd: 148250.45,
    fraud_rate: 0.042,
    flagged_count: 14,
    blocked_count: 7,
    active_alerts_count: 18,
    avg_amount: 296.5,
    p95_latency_ms: 19.4,
    sparklines: {
      tps: [
        { time: "2026-11-12T14:00:00Z", value: 0.62 },
        { time: "2026-11-12T14:30:00Z", value: 0.68 },
        { time: "2026-11-12T15:00:00Z", value: 0.72 },
      ],
      volume: [
        { time: "2026-11-12T14:00:00Z", value: 24500.0 },
        { time: "2026-11-12T14:30:00Z", value: 27100.0 },
        { time: "2026-11-12T15:00:00Z", value: 29400.0 },
      ],
      fraud_rate: [
        { time: "2026-11-12T14:00:00Z", value: 0.038 },
        { time: "2026-11-12T14:30:00Z", value: 0.045 },
        { time: "2026-11-12T15:00:00Z", value: 0.042 },
      ],
    },
  },
  volume: [
    {
      bucket_time: "2026-11-12T14:45:00Z",
      tx_count: 195,
      tps: 0.65,
      total_amount: 27400.0,
      avg_amount: 140.51,
      fraud_count: 7,
      fraud_rate: 0.0359,
    },
    {
      bucket_time: "2026-11-12T14:50:00Z",
      tx_count: 210,
      tps: 0.7,
      total_amount: 29820.0,
      avg_amount: 142.0,
      fraud_count: 9,
      fraud_rate: 0.0429,
    },
    {
      bucket_time: "2026-11-12T14:55:00Z",
      tx_count: 216,
      tps: 0.72,
      total_amount: 31104.0,
      avg_amount: 144.0,
      fraud_count: 9,
      fraud_rate: 0.0417,
    },
  ],
  fraudRate: {
    series: [
      { bucket_time: "2026-11-12T14:50:00Z", tx_count: 210, fraud_count: 9, fraud_rate: 0.0429 },
      { bucket_time: "2026-11-12T14:55:00Z", tx_count: 216, fraud_count: 9, fraud_rate: 0.0417 },
    ],
    risk_band_breakdown: [
      { risk_band: "low", count: 425, percentage: 85.0 },
      { risk_band: "medium", count: 54, percentage: 10.8 },
      { risk_band: "high", count: 14, percentage: 2.8 },
      { risk_band: "critical", count: 7, percentage: 1.4 },
    ],
  },
  categories: [
    {
      category: "money_transfer",
      tx_count: 42,
      total_amount: 142500.0,
      fraud_count: 15,
      fraud_rate: 0.3571,
      pct_of_volume: 42.5,
    },
    {
      category: "electronics",
      tx_count: 85,
      total_amount: 64200.0,
      fraud_count: 4,
      fraud_rate: 0.0471,
      pct_of_volume: 19.1,
    },
    {
      category: "groceries",
      tx_count: 140,
      total_amount: 18400.0,
      fraud_count: 1,
      fraud_rate: 0.0071,
      pct_of_volume: 5.5,
    },
  ],
  geo: [
    {
      country: "US",
      city: "New York",
      latitude: 40.7128,
      longitude: -74.006,
      tx_count: 112,
      total_amount: 48200.0,
      fraud_count: 4,
      fraud_rate: 0.0357,
    },
    {
      country: "RU",
      city: "Moscow",
      latitude: 55.7558,
      longitude: 37.6173,
      tx_count: 24,
      total_amount: 68400.0,
      fraud_count: 9,
      fraud_rate: 0.375,
    },
    {
      country: "GB",
      city: "London",
      latitude: 51.5074,
      longitude: -0.1278,
      tx_count: 68,
      total_amount: 29400.0,
      fraud_count: 2,
      fraud_rate: 0.0294,
    },
  ],
  anomalies: [
    {
      event_id: "550e8400-e29b-41d4-a716-446655440000",
      transaction_id: "550e8400-e29b-41d4-a716-446655440000",
      event_time: "2026-11-12T14:30:00.120Z",
      customer_id: "c1111111-e29b-41d4-a716-446655440001",
      merchant_id: "m2222222-e29b-41d4-a716-446655440002",
      amount: 9450.0,
      currency: "USD",
      fraud_score: 0.9125,
      risk_band: "critical",
      decision: "block",
      category: "money_transfer",
      triggered_rules: ["GEO_IMPOSSIBILITY", "INTERNATIONAL_HIGH_AMOUNT"],
    },
    {
      event_id: "550e8400-e29b-41d4-a716-446655440005",
      transaction_id: "550e8400-e29b-41d4-a716-446655440005",
      event_time: "2026-11-12T14:40:00.000Z",
      customer_id: "c1111111-e29b-41d4-a716-446655440002",
      merchant_id: "m2222222-e29b-41d4-a716-446655440003",
      amount: 8850.0,
      currency: "USD",
      fraud_score: 0.785,
      risk_band: "high",
      decision: "flag",
      category: "money_transfer",
      triggered_rules: ["STRUCTURING", "HIGH_RISK_MERCHANT"],
    },
  ],
  transactions: {
    items: [
      {
        event_id: "550e8400-e29b-41d4-a716-446655440000",
        transaction_id: "550e8400-e29b-41d4-a716-446655440000",
        event_time: "2026-11-12T14:30:00.120Z",
        customer_id: "c1111111-e29b-41d4-a716-446655440001",
        merchant_id: "m2222222-e29b-41d4-a716-446655440002",
        amount: 9450.0,
        currency: "USD",
        category: "money_transfer",
        fraud_score: 0.9125,
        risk_band: "critical",
        decision: "block",
        triggered_rules: ["GEO_IMPOSSIBILITY", "INTERNATIONAL_HIGH_AMOUNT"],
        feature_values: {
          txn_count_1m: 3,
          txn_count_5m: 4,
          txn_count_1h: 6,
          amount_mean_prior: 68.5,
          amount_std_prior: 24.1,
          amount_z_score: 389.27,
          travel_speed_kmph: 4820.5,
          is_warmup: false,
        },
        source_topic: "hermes.transactions.raw",
        kafka_partition: 2,
        kafka_offset: 10482,
        storage_destinations: ["postgres:transactions", "postgres:fraud_alerts", "redis:customer_state", "minio:parquet"],
      },
    ],
    total: 1,
    page: 1,
    page_size: 20,
    total_pages: 1,
  },
  alerts: [
    {
      alert_id: "alert-550e8400-e29b-41d4-a716-446655440000",
      event_id: "550e8400-e29b-41d4-a716-446655440000",
      event_time: "2026-11-12T14:30:00.120Z",
      customer_id: "c1111111-e29b-41d4-a716-446655440001",
      merchant_id: "m2222222-e29b-41d4-a716-446655440002",
      amount: 9450.0,
      currency: "USD",
      category: "money_transfer",
      fraud_score: 0.9125,
      risk_band: "critical",
      decision: "block",
      triggered_rules: ["GEO_IMPOSSIBILITY", "INTERNATIONAL_HIGH_AMOUNT"],
      status: "active",
    },
  ],
  customerProfile: {
    customer_id: "c1111111-e29b-41d4-a716-446655440001",
    full_name: "Customer 0001",
    email: "customer0001@hermes-bank.io",
    home_country: "US",
    home_city: "New York",
    home_latitude: 40.7128,
    home_longitude: -74.006,
    risk_tier: "high",
    card_type: "credit",
    account_created_at: "2025-02-14T00:00:00Z",
    prior_count: 12,
    is_warmup: false,
    amount_mean_prior: 118.4,
    amount_std_prior: 34.2,
    txn_count_1m: 1,
    txn_count_5m: 2,
    txn_count_1h: 4,
    category_breakdown: [
      { category: "money_transfer", tx_count: 3, total_amount: 26750.0 },
      { category: "electronics", tx_count: 5, total_amount: 2150.0 },
    ],
    recent_transactions: [
      {
        event_id: "550e8400-e29b-41d4-a716-446655440000",
        event_time: "2026-11-12T14:30:00.120Z",
        amount: 9450.0,
        category: "money_transfer",
        fraud_score: 0.9125,
        risk_band: "critical",
        decision: "block",
      },
    ],
    active_alerts: [],
  },
  fraudPerformance: {
    auc_roc: 0.9684,
    pr_auc: 0.9142,
    precision: 0.892,
    recall: 0.865,
    f1: 0.8783,
    confusion_matrix: { tp: 412, fp: 50, tn: 9474, fn: 64 },
    rule_trigger_frequencies: [
      { rule_id: "GEO_IMPOSSIBILITY", trigger_count: 36, pct_of_transactions: 7.2 },
      { rule_id: "AMOUNT_SPIKE", trigger_count: 35, pct_of_transactions: 7.0 },
      { rule_id: "STRUCTURING", trigger_count: 15, pct_of_transactions: 3.0 },
    ],
    score_distribution: [
      { bucket: "0.0-0.2", count: 380 },
      { bucket: "0.2-0.4", count: 45 },
      { bucket: "0.4-0.65", count: 54 },
      { bucket: "0.65-0.85", count: 14 },
      { bucket: "0.85-1.0", count: 7 },
    ],
    shadow_comparison: {
      champion_model: "xgboost-v1.0.0",
      shadow_model: "heuristic-shadow-v1.0.0",
      evaluated_count: 500,
      agreement_rate: 0.964,
      mean_score_delta: 0.0198,
    },
  },
  amlCompliance: {
    structuring_candidates: [
      {
        customer_id: "c1111111-e29b-41d4-a716-446655440001",
        structuring_tx_count: 3,
        total_structured_amount: 26850.0,
        max_fraud_score: 0.9125,
        sample_event_ids: ["550e8400-e29b-41d4-a716-446655440000"],
      },
    ],
    rapid_cross_border_hops: [
      {
        event_id: "550e8400-e29b-41d4-a716-446655440000",
        customer_id: "c1111111-e29b-41d4-a716-446655440001",
        event_time: "2026-11-12T14:30:00.120Z",
        country: "RU",
        city: "Moscow",
        amount: 9450.0,
        travel_speed_kmph: 4820.5,
        fraud_score: 0.9125,
      },
    ],
    high_value_international_transfers: [],
    summary: {
      structuring_customer_count: 5,
      rapid_hop_count: 12,
      high_value_international_count: 18,
    },
  },
  archiveSummary: {
    bucket: "hermes-archive",
    partition_scheme: "event_date=YYYY-MM-DD",
    compression: "SNAPPY",
    start_date: "2026-11-01",
    end_date: "2026-11-12",
    partition_count: 12,
    total_rows: 16800,
    total_volume_usd: 2326800.0,
    total_fraud_count: 588,
    partitions: [
      {
        partition: "event_date=2026-11-12",
        event_date: "2026-11-12",
        file_count: 2,
        row_count: 1500,
        total_amount: 207750.0,
        fraud_count: 52,
        compression: "SNAPPY",
      },
    ],
  },
};

async function requestJson(path, options = {}, fallbackData = null) {
  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    return payload && payload.data !== undefined ? payload.data : payload;
  } catch (_err) {
    return fallbackData;
  }
}

export async function getOverview(range = "24h") {
  return requestJson(`/overview?range=${encodeURIComponent(range)}`, {}, MOCK_FIXTURES.overview);
}

export async function getVolume(interval = "5m", range = "24h") {
  return requestJson(
    `/volume?interval=${encodeURIComponent(interval)}&range=${encodeURIComponent(range)}`,
    {},
    MOCK_FIXTURES.volume
  );
}

export async function getFraudRate(interval = "5m", range = "24h") {
  return requestJson(
    `/fraud-rate?interval=${encodeURIComponent(interval)}&range=${encodeURIComponent(range)}`,
    {},
    MOCK_FIXTURES.fraudRate
  );
}

export async function getCategories(range = "24h") {
  return requestJson(`/categories?range=${encodeURIComponent(range)}`, {}, MOCK_FIXTURES.categories);
}

export async function getGeo(range = "24h") {
  return requestJson(`/geo?range=${encodeURIComponent(range)}`, {}, MOCK_FIXTURES.geo);
}

export async function getAnomalies(limit = 200, range = "24h") {
  return requestJson(
    `/anomalies?limit=${encodeURIComponent(limit)}&range=${encodeURIComponent(range)}`,
    {},
    MOCK_FIXTURES.anomalies
  );
}

export async function getTransactions(params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") qs.set(k, String(v));
  });
  return requestJson(`/transactions?${qs.toString()}`, {}, MOCK_FIXTURES.transactions);
}

export async function getAlerts(status = "active", limit = 100) {
  return requestJson(
    `/alerts?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}`,
    {},
    MOCK_FIXTURES.alerts
  );
}

export async function acknowledgeAlert(alertId, analystNotes = "Acknowledged via Command Center") {
  return requestJson(
    `/alerts/${encodeURIComponent(alertId)}`,
    {
      method: "PATCH",
      body: JSON.stringify({ status: "acknowledged", analyst_notes: analystNotes }),
    },
    { alert_id: alertId, status: "acknowledged", analyst_notes: analystNotes }
  );
}

export async function dismissAlert(alertId, analystNotes = "Dismissed via Command Center") {
  return requestJson(
    `/alerts/${encodeURIComponent(alertId)}`,
    {
      method: "PATCH",
      body: JSON.stringify({ status: "dismissed", analyst_notes: analystNotes }),
    },
    { alert_id: alertId, status: "dismissed", analyst_notes: analystNotes }
  );
}

export async function getCustomerProfile(customerId) {
  return requestJson(
    `/customers/${encodeURIComponent(customerId)}/profile`,
    {},
    { ...MOCK_FIXTURES.customerProfile, customer_id: customerId }
  );
}

export async function getFraudPerformance(range = "7d") {
  return requestJson(
    `/fraud-performance?range=${encodeURIComponent(range)}`,
    {},
    MOCK_FIXTURES.fraudPerformance
  );
}

export async function getAmlCompliance(range = "7d") {
  return requestJson(
    `/compliance/aml?range=${encodeURIComponent(range)}`,
    {},
    MOCK_FIXTURES.amlCompliance
  );
}

export async function getArchiveSummary(startDate = "2026-11-01", endDate = "2026-11-12") {
  return requestJson(
    `/archive/summary?start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`,
    {},
    MOCK_FIXTURES.archiveSummary
  );
}
