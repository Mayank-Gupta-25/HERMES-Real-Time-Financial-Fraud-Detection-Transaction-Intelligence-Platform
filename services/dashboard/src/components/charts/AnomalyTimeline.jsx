import React, { useState } from "react";

const BAND_COLORS = {
  low: "#22c55e",
  medium: "#f59e0b",
  high: "#ef4444",
  critical: "#a855f7",
};

/**
 * D3/SVG AnomalyTimeline Scatter Plot Component (`Section 10.F.2` & `STEP AARUSSH-MOD07-01`)
 * Props: `{ anomalies: Array<{event_id, event_time, amount, fraud_score, risk_band, triggered_rules, category}>, loading: boolean }`
 */
export function AnomalyTimeline({ anomalies = [], loading = false }) {
  const [selectedAnomaly, setSelectedAnomaly] = useState(null);

  if (loading) {
    return (
      <div className="glass-panel p-4" data-testid="anomaly-timeline-loading">
        <div className="text-xs text-slate-400">Loading anomaly scatter timeline...</div>
      </div>
    );
  }

  const maxAmount = Math.max(...anomalies.map((a) => Number(a.amount) || 100), 100);

  return (
    <div className="glass-panel p-4" data-testid="anomaly-timeline-chart">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            Anomaly Severity Scatter Timeline
          </h3>
          <p className="text-xs text-slate-400">
            X = event_time · Y = amount ($) · Radius = fraud_score · Color = risk_band
          </p>
        </div>
        <span className="text-xs font-mono text-slate-400">
          {anomalies.length} anomalies plotted
        </span>
      </div>

      {anomalies.length === 0 ? (
        <div className="py-8 text-center text-xs text-slate-400" data-testid="anomaly-empty-state">
          No anomalous transactions detected in window.
        </div>
      ) : (
        <div>
          <svg
            viewBox="0 0 680 220"
            className="w-full h-48 bg-slate-950/80 rounded-lg border border-slate-800"
            data-testid="anomaly-timeline-svg"
          >
            <line x1="40" y1="190" x2="650" y2="190" stroke="#334155" />
            <line x1="40" y1="20" x2="40" y2="190" stroke="#334155" />

            {anomalies.map((item, idx) => {
              const x =
                55 +
                Math.round(
                  (idx / Math.max(anomalies.length - 1, 1)) * 575
                );
              const y =
                180 -
                Math.round(
                  (Math.min(Number(item.amount) || 0, maxAmount) / maxAmount) * 150
                );
              const r = Math.max(5, Math.round((Number(item.fraud_score) || 0.5) * 11));
              const band = (item.risk_band || "high").toLowerCase();
              const color = BAND_COLORS[band] || BAND_COLORS.high;

              return (
                <circle
                  key={item.event_id || idx}
                  cx={x}
                  cy={y}
                  r={r}
                  fill={color}
                  fillOpacity="0.78"
                  stroke="#0f172a"
                  strokeWidth="1.5"
                  data-testid="anomaly-point-circle"
                  onMouseEnter={() => setSelectedAnomaly(item)}
                  onClick={() => setSelectedAnomaly(item)}
                />
              );
            })}
          </svg>

          {selectedAnomaly && (
            <div
              className="mt-2 p-2.5 rounded bg-slate-900 border border-slate-700 text-xs flex flex-wrap items-center justify-between gap-2"
              data-testid="anomaly-tooltip"
            >
              <span className="font-mono text-blue-400">
                {selectedAnomaly.event_id}
              </span>
              <span className="font-semibold text-slate-100">
                ${Number(selectedAnomaly.amount || 0).toLocaleString("en-US")}
              </span>
              <span>
                Score: {Number(selectedAnomaly.fraud_score || 0).toFixed(4)} ({selectedAnomaly.risk_band})
              </span>
              <span>
                Rules: {(selectedAnomaly.triggered_rules || []).join(", ") || "ML Anomaly"}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default AnomalyTimeline;
