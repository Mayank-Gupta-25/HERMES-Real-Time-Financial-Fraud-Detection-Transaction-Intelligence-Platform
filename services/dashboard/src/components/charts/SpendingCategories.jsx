import React from "react";

/**
 * SpendingCategories Chart Component (`STEP KUSHAGRA-MOD07-02`)
 * Displays merchant category share of volume and category fraud rate %.
 */
export function SpendingCategories({ categories = [], loading = false }) {
  if (loading) {
    return (
      <div className="glass-panel p-4" data-testid="spending-categories-loading">
        <div className="text-xs text-slate-400">Loading spending categories...</div>
      </div>
    );
  }

  return (
    <div className="glass-panel p-4" data-testid="spending-categories-chart">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            Merchant Category Volume & Fraud Breakdown
          </h3>
          <p className="text-xs text-slate-400">
            Dual view: volume share (%) & category fraud rate (%)
          </p>
        </div>
      </div>

      {categories.length === 0 ? (
        <div className="py-6 text-center text-xs text-slate-400">
          No category metrics available.
        </div>
      ) : (
        <div className="space-y-3">
          {categories.map((cat) => {
            const fraudPct = Number(((Number(cat.fraud_rate) || 0) * 100).toFixed(1));
            const volPct = Number(cat.pct_of_volume || 0).toFixed(1);
            const barColor =
              fraudPct >= 20
                ? "bg-purple-500"
                : fraudPct >= 8
                ? "bg-red-500"
                : fraudPct >= 3
                ? "bg-amber-500"
                : "bg-emerald-500";

            return (
              <div
                key={cat.category}
                data-testid={`category-row-${cat.category}`}
                className="space-y-1 text-xs"
              >
                <div className="flex items-center justify-between">
                  <span className="font-medium text-slate-200 capitalize">
                    {String(cat.category).replace(/_/g, " ")}
                  </span>
                  <div className="flex items-center gap-3 text-[11px] font-mono">
                    <span className="text-slate-400">
                      ${Number(cat.total_amount || 0).toLocaleString("en-US")} ({volPct}%)
                    </span>
                    <span className="text-red-300 font-semibold">
                      Fraud: {fraudPct}% ({cat.fraud_count || 0}/{cat.tx_count || 0})
                    </span>
                  </div>
                </div>
                <div className="w-full h-2 bg-slate-800 rounded overflow-hidden flex">
                  <div
                    style={{ width: `${Math.min(100, Math.max(4, Number(volPct)))}%` }}
                    className="h-full bg-blue-500/70"
                  />
                  <div
                    style={{ width: `${Math.min(100, fraudPct)}%` }}
                    className={`h-full ${barColor}`}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default SpendingCategories;
