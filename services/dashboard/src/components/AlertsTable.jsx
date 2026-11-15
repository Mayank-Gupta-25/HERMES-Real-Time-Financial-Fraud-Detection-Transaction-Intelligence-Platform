import React from "react";

/**
 * Live Fraud AlertsTable Component (`Section 10.F.2` & `STEP MAYANK-MOD07-01`)
 * Props: `{ alerts, onAcknowledge, onDismiss }`
 */
export function AlertsTable({
  alerts = [],
  onAcknowledge = async () => {},
  onDismiss = async () => {},
}) {
  return (
    <div className="glass-panel p-4" data-testid="alerts-table-container">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-red-500 animate-ping" />
          <h3 className="text-base font-semibold text-slate-100">
            Real-Time Fraud Alert Queue
          </h3>
        </div>
        <span className="text-xs text-slate-400">
          {alerts.length} alerts in queue
        </span>
      </div>

      {alerts.length === 0 ? (
        <div className="py-6 text-center text-xs text-slate-400">
          No active fraud alerts.
        </div>
      ) : (
        <div className="space-y-2 max-h-96 overflow-y-auto pr-1" data-testid="alerts-list">
          {alerts.map((alert) => {
            const aid = alert.alert_id || `alert-${alert.event_id}`;
            const status = (alert.status || "active").toLowerCase();
            const band = (alert.risk_band || alert.risk_level || "high").toLowerCase();

            return (
              <div
                key={aid}
                data-testid={`alert-row-${aid}`}
                className="p-3 rounded-lg bg-slate-900/90 border border-slate-800 flex flex-wrap items-center justify-between gap-3"
              >
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <span
                      className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${
                        band === "critical"
                          ? "bg-purple-950 text-purple-300 border border-purple-700"
                          : "bg-red-950 text-red-300 border border-red-700"
                      }`}
                    >
                      {band}
                    </span>
                    <span className="font-mono text-xs text-slate-200 font-semibold">
                      {aid}
                    </span>
                    <span className="text-xs text-slate-400">
                      Score: {Number(alert.fraud_score || 0).toFixed(4)}
                    </span>
                    <span className="text-xs font-semibold text-slate-100">
                      ${Number(alert.amount || 0).toLocaleString("en-US", { minimumFractionDigits: 2 })}
                    </span>
                    <span className="text-[11px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-300">
                      Status: {status}
                    </span>
                  </div>

                  <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-slate-400">
                    <span>Customer: {String(alert.customer_id || "").slice(0, 12)}</span>
                    <span>·</span>
                    <span>Rules:</span>
                    {(alert.triggered_rules || []).map((rule) => (
                      <span
                        key={rule}
                        className="px-1.5 py-0.5 rounded bg-red-950/50 text-red-200 border border-red-800/50 text-[10px]"
                      >
                        {rule}
                      </span>
                    ))}
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    data-testid={`ack-btn-${aid}`}
                    disabled={status === "acknowledged"}
                    onClick={() => onAcknowledge(aid)}
                    className="px-2.5 py-1 rounded text-xs font-medium bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white"
                  >
                    Acknowledge
                  </button>
                  <button
                    type="button"
                    data-testid={`dismiss-btn-${aid}`}
                    disabled={status === "dismissed"}
                    onClick={() => onDismiss(aid)}
                    className="px-2.5 py-1 rounded text-xs font-medium bg-slate-800 hover:bg-slate-700 disabled:opacity-40 text-slate-200 border border-slate-700"
                  >
                    Dismiss
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default AlertsTable;
