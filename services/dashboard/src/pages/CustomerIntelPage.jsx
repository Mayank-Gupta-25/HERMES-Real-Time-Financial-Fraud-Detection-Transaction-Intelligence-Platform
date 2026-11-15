import React, { useEffect, useState } from "react";
import SpendingCategories from "../components/charts/SpendingCategories.jsx";
import {
  getAmlCompliance,
  getArchiveSummary,
  getCategories,
  getCustomerProfile,
  MOCK_FIXTURES,
} from "../services/api.js";

/**
 * Customer 360 Intelligence & AML Compliance Page (`STEP KUSHAGRA-MOD07-02`)
 * Displays Customer 360 lookup, Welford prior baseline (`amount_mean_prior`, `amount_std_prior`, `is_warmup`),
 * live `1m`/`5m`/`1h` velocity counters, category breakdown, and AML Structuring & Parquet Cold Archive panel.
 */
export function CustomerIntelPage() {
  const [customerIdInput, setCustomerIdInput] = useState(
    MOCK_FIXTURES.customerProfile.customer_id
  );
  const [profile, setProfile] = useState(MOCK_FIXTURES.customerProfile);
  const [aml, setAml] = useState(MOCK_FIXTURES.amlCompliance);
  const [archive, setArchive] = useState(MOCK_FIXTURES.archiveSummary);
  const [categories, setCategories] = useState(MOCK_FIXTURES.categories);

  useEffect(() => {
    let cancelled = false;
    async function loadInitial() {
      const [profRes, amlRes, archRes, catRes] = await Promise.all([
        getCustomerProfile(customerIdInput),
        getAmlCompliance("7d"),
        getArchiveSummary("2026-11-01", "2026-11-12"),
        getCategories("24h"),
      ]);
      if (!cancelled) {
        if (profRes) setProfile(profRes);
        if (amlRes) setAml(amlRes);
        if (archRes) setArchive(archRes);
        if (Array.isArray(catRes)) setCategories(catRes);
      }
    }
    loadInitial();
    return () => {
      cancelled = true;
    };
  }, []);

  async function handleCustomerLookup(e) {
    if (e && e.preventDefault) e.preventDefault();
    if (!customerIdInput.trim()) return;
    const profRes = await getCustomerProfile(customerIdInput.trim());
    if (profRes) setProfile(profRes);
  }

  return (
    <div className="space-y-4" data-testid="customer-intel-page">
      {/* Top Search Bar */}
      <form
        onSubmit={handleCustomerLookup}
        className="glass-panel p-4 flex flex-wrap items-center justify-between gap-3"
      >
        <div>
          <h2 className="text-lg font-bold text-slate-100">
            Customer 360 Risk Profile & AML Compliance Watchlist
          </h2>
          <p className="text-xs text-slate-400">
            PostgreSQL Operational History + Redis Welford Prior Baseline + MinIO Parquet Archive
          </p>
        </div>

        <div className="flex items-center gap-2">
          <input
            type="text"
            value={customerIdInput}
            onChange={(e) => setCustomerIdInput(e.target.value)}
            placeholder="Enter customer UUID..."
            data-testid="customer-search-input"
            className="bg-slate-900 border border-slate-700 rounded px-3 py-1.5 text-xs text-slate-100 font-mono w-72"
          />
          <button
            type="submit"
            data-testid="customer-lookup-btn"
            className="px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
          >
            Lookup Customer 360
          </button>
        </div>
      </form>

      {/* Customer 360 Profile & Welford Baseline */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="glass-panel p-4 space-y-3" data-testid="customer-360-card">
          <div className="flex items-center justify-between">
            <span className="text-sm font-bold text-slate-100">{profile?.full_name}</span>
            <span className="px-2 py-0.5 rounded bg-red-950 text-red-300 border border-red-800 text-xs uppercase font-semibold">
              Tier: {profile?.risk_tier || "low"}
            </span>
          </div>
          <div className="text-xs text-slate-400 font-mono space-y-1">
            <div>ID: {profile?.customer_id}</div>
            <div>Email: {profile?.email}</div>
            <div>
              Home: {profile?.home_city}, {profile?.home_country}
            </div>
            <div>Card Type: {profile?.card_type}</div>
          </div>

          <div className="p-3 rounded bg-slate-900 border border-slate-800 text-xs space-y-1">
            <div className="font-semibold text-blue-400">
              Leakage-Free Welford Prior Baseline
            </div>
            <div className="flex justify-between">
              <span className="text-slate-400">Prior Event Count:</span>
              <span className="font-mono text-slate-100">
                {profile?.prior_count} ({profile?.is_warmup ? "WARMUP < 5" : "ACTIVE"})
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-slate-400">Prior Mean (μ):</span>
              <span className="font-mono text-emerald-400">
                ${Number(profile?.amount_mean_prior || 0).toFixed(2)}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-slate-400">Prior Std Dev (σ):</span>
              <span className="font-mono text-amber-400">
                ${Number(profile?.amount_std_prior || 0).toFixed(2)}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-3 gap-2 text-center text-xs">
            <div className="p-2 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">1m Vel</div>
              <div className="font-bold text-slate-100">{profile?.txn_count_1m ?? 1}</div>
            </div>
            <div className="p-2 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">5m Vel</div>
              <div className="font-bold text-slate-100">{profile?.txn_count_5m ?? 2}</div>
            </div>
            <div className="p-2 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">1h Vel</div>
              <div className="font-bold text-slate-100">{profile?.txn_count_1h ?? 4}</div>
            </div>
          </div>
        </div>

        <div className="lg:col-span-2">
          <SpendingCategories categories={categories} />
        </div>
      </div>

      {/* Bottom Panel: AML Structuring & Cold Parquet Archive Summary */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="glass-panel p-4" data-testid="aml-watchlist-panel">
          <h3 className="text-base font-semibold text-slate-100 mb-2">
            AML Structuring & Rapid Cross-Border Watchlist
          </h3>
          <p className="text-xs text-slate-400 mb-3">
            Structuring ($8,000–$9,999 repeated transfers) · Rapid Country Hops
          </p>
          <div className="space-y-2 text-xs">
            {(aml?.structuring_candidates || []).map((cand) => (
              <div
                key={cand.customer_id}
                data-testid={`aml-candidate-${cand.customer_id}`}
                className="p-2.5 rounded bg-slate-900 border border-slate-800 flex items-center justify-between"
              >
                <span className="font-mono text-blue-400">{cand.customer_id}</span>
                <span>Tx Count: {cand.structuring_tx_count}</span>
                <span className="font-semibold text-amber-300">
                  Structured Total: ${Number(cand.total_structured_amount || 0).toLocaleString("en-US")}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-panel p-4" data-testid="parquet-archive-panel">
          <h3 className="text-base font-semibold text-slate-100 mb-2">
            MinIO Cold Parquet Archive Summary
          </h3>
          <p className="text-xs text-slate-400 mb-3">
            Bucket: {archive?.bucket} · Partitioning: {archive?.partition_scheme} · Compression: {archive?.compression}
          </p>
          <div className="grid grid-cols-3 gap-2 text-center text-xs mb-3">
            <div className="p-2.5 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">Partitions</div>
              <div className="font-bold text-slate-100">{archive?.partition_count}</div>
            </div>
            <div className="p-2.5 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">Archived Rows</div>
              <div className="font-bold text-blue-400">
                {Number(archive?.total_rows || 0).toLocaleString("en-US")}
              </div>
            </div>
            <div className="p-2.5 rounded bg-slate-900 border border-slate-800">
              <div className="text-slate-400">Archived Volume</div>
              <div className="font-bold text-emerald-400">
                ${Number(archive?.total_volume_usd || 0).toLocaleString("en-US")}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default CustomerIntelPage;
