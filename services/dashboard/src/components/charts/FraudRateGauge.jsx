import React from "react";

/**
 * FraudRateGauge Component (`Section 10.F.2`)
 * Props: `{ fraudRate: number, flaggedCount: number, blockedCount: number, totalCount: number }`
 */
export function FraudRateGauge({
  fraudRate = 0,
  flaggedCount = 0,
  blockedCount = 0,
  totalCount = 0,
}) {
  const ratePct = Number((Number(fraudRate || 0) * 100).toFixed(2));
  const clamped = Math.min(100, Math.max(0, ratePct * 5));
  const gaugeColor =
    ratePct >= 8 ? "#a855f7" : ratePct >= 5 ? "#ef4444" : ratePct >= 2.5 ? "#f59e0b" : "#22c55e";

  return (
    <div className="glass-panel p-4 flex flex-col justify-between" data-testid="fraud-rate-gauge">
      <div>
        <h3 className="text-base font-semibold text-slate-100">
          Hybrid Fraud Rate & Decision Split
        </h3>
        <p className="text-xs text-slate-400">
          Live risk ratio across {totalCount} evaluated events
        </p>
      </div>

      <div className="my-4 flex flex-col items-center">
        <svg viewBox="0 0 200 110" className="w-48 h-28">
          <path
            d="M 20 95 A 80 80 0 0 1 180 95"
            fill="none"
            stroke="#1f2937"
            strokeWidth="16"
            strokeLinecap="round"
          />
          <path
            d="M 20 95 A 80 80 0 0 1 180 95"
            fill="none"
            stroke={gaugeColor}
            strokeWidth="16"
            strokeLinecap="round"
            strokeDasharray={`${(clamped / 100) * 251.2} 251.2`}
          />
          <text
            x="100"
            y="85"
            textAnchor="middle"
            className="fill-slate-100 text-2xl font-bold"
            data-testid="gauge-rate-pct"
          >
            {ratePct}%
          </text>
        </svg>
      </div>

      <div className="grid grid-cols-3 gap-2 text-center text-xs">
        <div className="p-2 rounded bg-slate-900 border border-slate-800">
          <div className="text-slate-400">Approved</div>
          <div className="text-emerald-400 font-bold text-sm">
            {Math.max(0, totalCount - flaggedCount - blockedCount)}
          </div>
        </div>
        <div className="p-2 rounded bg-slate-900 border border-slate-800">
          <div className="text-slate-400">Flagged</div>
          <div className="text-amber-400 font-bold text-sm" data-testid="gauge-flagged-count">
            {flaggedCount}
          </div>
        </div>
        <div className="p-2 rounded bg-slate-900 border border-slate-800">
          <div className="text-slate-400">Blocked</div>
          <div className="text-red-400 font-bold text-sm" data-testid="gauge-blocked-count">
            {blockedCount}
          </div>
        </div>
      </div>
    </div>
  );
}

export default FraudRateGauge;
