import React from "react";

/**
 * KPI MetricCard Component (`Section 10.F.2` Contract)
 * Props: `{ title, value, unit, delta, sparklineData, status }`
 */
export function MetricCard({
  title,
  value,
  unit = "",
  delta = null,
  sparklineData = [],
  status = "normal",
}) {
  const statusStyles = {
    normal: "border-slate-800 text-emerald-400",
    warning: "border-amber-500/40 text-amber-400",
    critical: "border-purple-500/50 text-purple-400",
  };
  const accentClass = statusStyles[status] || statusStyles.normal;

  return (
    <div
      className={`glass-panel p-4 border ${accentClass} flex flex-col justify-between`}
      data-testid="metric-card"
    >
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wider text-slate-400">
          {title}
        </span>
        {delta !== null && delta !== undefined && (
          <span
            className={`text-xs font-semibold px-1.5 py-0.5 rounded ${
              Number(delta) >= 0
                ? "bg-emerald-950/70 text-emerald-300"
                : "bg-red-950/70 text-red-300"
            }`}
          >
            {Number(delta) >= 0 ? `+${delta}%` : `${delta}%`}
          </span>
        )}
      </div>

      <div className="mt-2 flex items-baseline gap-1.5">
        <span className="text-2xl font-bold text-slate-100" data-testid="metric-card-value">
          {value}
        </span>
        {unit && <span className="text-xs text-slate-400 font-medium">{unit}</span>}
      </div>

      {Array.isArray(sparklineData) && sparklineData.length > 0 && (
        <div
          className="mt-3 flex items-end gap-1 h-7"
          data-testid="metric-card-sparkline"
          aria-label={`${title} sparkline`}
        >
          {sparklineData.slice(-12).map((pt, idx) => {
            const maxVal = Math.max(...sparklineData.map((d) => Number(d.value) || 1), 1);
            const heightPct = Math.max(15, Math.round(((Number(pt.value) || 0) / maxVal) * 100));
            return (
              <div
                key={pt.time || idx}
                style={{ height: `${heightPct}%` }}
                className="flex-1 rounded-sm bg-blue-500/60"
              />
            );
          })}
        </div>
      )}
    </div>
  );
}

export default MetricCard;
