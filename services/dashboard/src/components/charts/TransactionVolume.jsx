import React from "react";

/**
 * TransactionVolume Chart Component (`Section 10.F.2`)
 * Props: `{ data: Array<{bucket_time, tps, tx_count, fraud_count}>, loading: boolean }`
 */
export function TransactionVolume({ data = [], loading = false }) {
  if (loading) {
    return (
      <div className="glass-panel p-4" data-testid="tx-volume-loading">
        <div className="text-sm text-slate-400">Loading transaction volume stream...</div>
      </div>
    );
  }

  const points = Array.isArray(data) ? data.slice(-24) : [];
  const maxTx = Math.max(...points.map((d) => Number(d.tx_count) || 1), 1);

  return (
    <div className="glass-panel p-4" data-testid="transaction-volume-chart">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            Real-Time Transaction Volume & TPS
          </h3>
          <p className="text-xs text-slate-400">
            TimescaleDB 5-Minute Continuous Aggregate Buckets
          </p>
        </div>
        {points.length > 0 && (
          <div className="text-xs font-mono text-blue-400">
            Latest TPS: {Number(points[points.length - 1].tps || 0).toFixed(2)}
          </div>
        )}
      </div>

      {points.length === 0 ? (
        <div className="py-8 text-center text-xs text-slate-400">
          No volume buckets available.
        </div>
      ) : (
        <svg
          viewBox="0 0 600 180"
          className="w-full h-44 overflow-visible"
          role="img"
          aria-label="Transaction Volume Time Series"
        >
          {points.map((pt, idx) => {
            const barWidth = Math.max(8, Math.floor(540 / Math.max(points.length, 1)) - 4);
            const x = 30 + idx * (barWidth + 4);
            const barHeight = Math.max(6, Math.round(((Number(pt.tx_count) || 0) / maxTx) * 140));
            const y = 160 - barHeight;
            const fraudHeight = Math.min(
              barHeight,
              Math.max(2, Math.round(((Number(pt.fraud_count) || 0) / maxTx) * 140 * 4))
            );

            return (
              <g key={pt.bucket_time || idx} data-testid="volume-bar">
                <rect
                  x={x}
                  y={y}
                  width={barWidth}
                  height={barHeight}
                  rx="2"
                  fill="#3b82f6"
                  fillOpacity="0.65"
                />
                {Number(pt.fraud_count) > 0 && (
                  <rect
                    x={x}
                    y={160 - fraudHeight}
                    width={barWidth}
                    height={fraudHeight}
                    rx="2"
                    fill="#ef4444"
                  />
                )}
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}

export default TransactionVolume;
