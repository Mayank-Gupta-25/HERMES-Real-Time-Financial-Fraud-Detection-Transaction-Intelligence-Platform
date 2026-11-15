import React, { useState } from "react";

const RISK_BADGE_CLASSES = {
  low: "bg-emerald-950/70 text-emerald-300 border-emerald-700/60",
  medium: "bg-amber-950/70 text-amber-300 border-amber-700/60",
  high: "bg-red-950/70 text-red-300 border-red-700/60",
  critical: "bg-purple-950/70 text-purple-300 border-purple-700/60",
};

const DECISION_BADGE_CLASSES = {
  approve: "bg-emerald-900/40 text-emerald-300",
  flag: "bg-amber-900/50 text-amber-300",
  block: "bg-red-900/60 text-red-200",
};

/**
 * Operational TransactionTable Component (`Section 10.F.2` & `STEP MAYANK-MOD07-01`)
 * Props: `{ transactions, totalCount, page, pageSize, onPageChange, onFilterChange, loading }`
 */
export function TransactionTable({
  transactions = [],
  totalCount = 0,
  page = 1,
  pageSize = 20,
  onPageChange = () => {},
  onFilterChange = () => {},
  loading = false,
}) {
  const [expandedEventId, setExpandedEventId] = useState(null);
  const [riskBandFilter, setRiskBandFilter] = useState("");
  const [decisionFilter, setDecisionFilter] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [searchQuery, setSearchQuery] = useState("");

  function handleFilterUpdate(nextOverrides = {}) {
    const nextFilters = {
      risk_band: nextOverrides.risk_band !== undefined ? nextOverrides.risk_band : riskBandFilter,
      decision: nextOverrides.decision !== undefined ? nextOverrides.decision : decisionFilter,
      category: nextOverrides.category !== undefined ? nextOverrides.category : categoryFilter,
      customer_id: nextOverrides.customer_id !== undefined ? nextOverrides.customer_id : searchQuery,
    };
    onFilterChange(nextFilters);
  }

  return (
    <div className="glass-panel p-4" data-testid="transaction-table-container">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            Operational Scored Transactions
          </h3>
          <p className="text-xs text-slate-400">
            Total Matched: {totalCount || transactions.length} · Click any row to inspect audit provenance & 16 features
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <input
            type="text"
            placeholder="Filter customer_id or event_id..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value);
              handleFilterUpdate({ customer_id: e.target.value });
            }}
            data-testid="tx-search-input"
            className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-200"
          />

          <select
            aria-label="Filter by Risk Band"
            value={riskBandFilter}
            onChange={(e) => {
              setRiskBandFilter(e.target.value);
              handleFilterUpdate({ risk_band: e.target.value });
            }}
            data-testid="tx-risk-filter"
            className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-200"
          >
            <option value="">All Risk Bands</option>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
            <option value="critical">Critical</option>
          </select>

          <select
            aria-label="Filter by Decision"
            value={decisionFilter}
            onChange={(e) => {
              setDecisionFilter(e.target.value);
              handleFilterUpdate({ decision: e.target.value });
            }}
            data-testid="tx-decision-filter"
            className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-200"
          >
            <option value="">All Decisions</option>
            <option value="approve">Approve</option>
            <option value="flag">Flag</option>
            <option value="block">Block</option>
          </select>

          <select
            aria-label="Filter by Category"
            value={categoryFilter}
            onChange={(e) => {
              setCategoryFilter(e.target.value);
              handleFilterUpdate({ category: e.target.value });
            }}
            className="bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-slate-200"
          >
            <option value="">All Categories</option>
            <option value="money_transfer">Money Transfer</option>
            <option value="electronics">Electronics</option>
            <option value="groceries">Groceries</option>
            <option value="gambling">Gambling</option>
            <option value="crypto">Crypto</option>
          </select>
        </div>
      </div>

      {loading ? (
        <div className="py-8 text-center text-sm text-slate-400">
          Loading scored transactions...
        </div>
      ) : transactions.length === 0 ? (
        <div className="py-8 text-center text-sm text-slate-400">
          No transactions match the selected filters.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs text-slate-300" data-testid="transaction-table">
            <thead className="uppercase text-[11px] text-slate-400 border-b border-slate-800 bg-slate-900/50">
              <tr>
                <th className="py-2.5 px-3">Event ID</th>
                <th className="py-2.5 px-3">Timestamp (UTC)</th>
                <th className="py-2.5 px-3">Customer ID</th>
                <th className="py-2.5 px-3">Category</th>
                <th className="py-2.5 px-3">Amount</th>
                <th className="py-2.5 px-3">Fraud Score</th>
                <th className="py-2.5 px-3">Risk Band</th>
                <th className="py-2.5 px-3">Decision</th>
                <th className="py-2.5 px-3">Triggered Rules</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/70">
              {transactions.map((tx) => {
                const eid = tx.event_id || tx.transaction_id;
                const isExpanded = expandedEventId === eid;
                const band = (tx.risk_band || tx.risk_level || "low").toLowerCase();
                const dec = (tx.decision || "approve").toLowerCase();
                const scorePct = Math.min(100, Math.round(Number(tx.fraud_score || 0) * 100));
                const provenance = tx.provenance || {
                  source_topic: tx.source_topic || "hermes.transactions.raw",
                  partition: tx.kafka_partition ?? 0,
                  offset: tx.kafka_offset ?? 0,
                  storage_destinations: tx.storage_destinations || [
                    "postgres:transactions",
                    "redis:customer_state",
                  ],
                };

                return (
                  <React.Fragment key={eid}>
                    <tr
                      onClick={() => setExpandedEventId(isExpanded ? null : eid)}
                      data-testid={`tx-row-${eid}`}
                      className="hover:bg-slate-800/40 cursor-pointer transition"
                    >
                      <td className="py-2.5 px-3 font-mono text-blue-400">
                        {String(eid).slice(0, 12)}...
                      </td>
                      <td className="py-2.5 px-3 font-mono">{tx.event_time || tx.timestamp}</td>
                      <td className="py-2.5 px-3 font-mono">
                        {String(tx.customer_id).slice(0, 12)}...
                      </td>
                      <td className="py-2.5 px-3">{tx.category}</td>
                      <td className="py-2.5 px-3 font-semibold text-slate-100">
                        ${Number(tx.amount || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })} {tx.currency || "USD"}
                      </td>
                      <td className="py-2.5 px-3">
                        <div className="flex items-center gap-2">
                          <div className="w-16 h-2 bg-slate-800 rounded overflow-hidden">
                            <div
                              style={{ width: `${scorePct}%` }}
                              className={`h-full ${
                                scorePct >= 85
                                  ? "bg-purple-500"
                                  : scorePct >= 65
                                  ? "bg-red-500"
                                  : scorePct >= 40
                                  ? "bg-amber-500"
                                  : "bg-emerald-500"
                              }`}
                            />
                          </div>
                          <span className="font-mono">{Number(tx.fraud_score || 0).toFixed(4)}</span>
                        </div>
                      </td>
                      <td className="py-2.5 px-3">
                        <span
                          className={`px-2 py-0.5 rounded border text-[11px] font-semibold uppercase ${
                            RISK_BADGE_CLASSES[band] || RISK_BADGE_CLASSES.low
                          }`}
                          data-testid={`risk-badge-${eid}`}
                        >
                          {band}
                        </span>
                      </td>
                      <td className="py-2.5 px-3">
                        <span
                          className={`px-2 py-0.5 rounded text-[11px] font-semibold uppercase ${
                            DECISION_BADGE_CLASSES[dec] || DECISION_BADGE_CLASSES.approve
                          }`}
                        >
                          {dec}
                        </span>
                      </td>
                      <td className="py-2.5 px-3">
                        <div className="flex flex-wrap gap-1">
                          {(tx.triggered_rules || []).length === 0 ? (
                            <span className="text-slate-500">None</span>
                          ) : (
                            (tx.triggered_rules || []).map((rule) => (
                              <span
                                key={rule}
                                className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-200 border border-slate-700 text-[10px]"
                              >
                                {rule}
                              </span>
                            ))
                          )}
                        </div>
                      </td>
                    </tr>

                    {isExpanded && (
                      <tr className="bg-slate-900/90" data-testid={`provenance-drawer-${eid}`}>
                        <td colSpan={9} className="p-4">
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                            <div className="p-3 rounded bg-slate-950 border border-slate-800">
                              <div className="font-semibold text-blue-400 mb-2">
                                Audit Provenance Metadata (NFR-05)
                              </div>
                              <div className="space-y-1 font-mono text-[11px]">
                                <div>source_topic: {provenance.source_topic}</div>
                                <div>partition: {provenance.partition}</div>
                                <div>offset: {provenance.offset}</div>
                                <div>
                                  storage_destinations:{" "}
                                  {(provenance.storage_destinations || []).join(", ")}
                                </div>
                                <div>model_version: {tx.model_version || "xgboost-v1.0.0"}</div>
                                <div>rule_version: {tx.rule_version || "rules-v1.0.0"}</div>
                              </div>
                            </div>

                            <div className="p-3 rounded bg-slate-950 border border-slate-800">
                              <div className="font-semibold text-emerald-400 mb-2">
                                Extracted Feature Vector (feature_values)
                              </div>
                              <div className="grid grid-cols-2 gap-1 font-mono text-[11px]">
                                {Object.entries(tx.feature_values || {}).map(([fk, fv]) => (
                                  <div key={fk}>
                                    <span className="text-slate-400">{fk}:</span>{" "}
                                    <span className="text-slate-200">{String(fv)}</span>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="mt-4 flex items-center justify-between text-xs text-slate-400">
        <span>
          Page {page} · Showing {transactions.length} of {totalCount || transactions.length}
        </span>
        <div className="flex gap-2">
          <button
            type="button"
            disabled={page <= 1}
            onClick={() => onPageChange(Math.max(1, page - 1))}
            className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 disabled:opacity-40 text-slate-200"
          >
            Previous
          </button>
          <button
            type="button"
            onClick={() => onPageChange(page + 1)}
            className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}

export default TransactionTable;
