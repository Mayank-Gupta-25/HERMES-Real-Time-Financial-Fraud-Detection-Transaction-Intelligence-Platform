import React, { useState } from "react";

/**
 * D3/SVG GeoHeatmap Component (`Section 10.F.2` & `STEP AARUSSH-MOD07-01`)
 * Props: `{ geoPoints: Array<{country, city, latitude, longitude, tx_count, fraud_rate, total_amount}>, loading: boolean }`
 */
export function GeoHeatmap({ geoPoints = [], loading = false }) {
  const [hoveredPoint, setHoveredPoint] = useState(null);

  if (loading) {
    return (
      <div className="glass-panel p-4" data-testid="geo-heatmap-loading">
        <div className="text-xs text-slate-400">Loading geospatial fraud density map...</div>
      </div>
    );
  }

  function projectCoords(lat, lon) {
    const x = ((Number(lon) + 180) / 360) * 640 + 20;
    const y = ((90 - Number(lat)) / 180) * 260 + 20;
    return { x: Math.round(x * 10) / 10, y: Math.round(y * 10) / 10 };
  }

  function riskColor(fraudRate) {
    const r = Number(fraudRate) || 0;
    if (r >= 0.25) return "#a855f7"; // critical
    if (r >= 0.1) return "#ef4444"; // high
    if (r >= 0.04) return "#f59e0b"; // medium
    return "#22c55e"; // low
  }

  return (
    <div className="glass-panel p-4" data-testid="geo-heatmap-chart">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            Global Geospatial Fraud Density Overlay
          </h3>
          <p className="text-xs text-slate-400">
            Bubble radius = sqrt(tx_count) · Color = regional fraud rate severity
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-slate-400">
          <span className="text-emerald-400">● Low</span>
          <span className="text-amber-400">● Med</span>
          <span className="text-red-400">● High</span>
          <span className="text-purple-400">● Crit</span>
        </div>
      </div>

      {geoPoints.length === 0 ? (
        <div className="py-8 text-center text-xs text-slate-400" data-testid="geo-empty-state">
          No geographic points available.
        </div>
      ) : (
        <div className="relative">
          <svg
            viewBox="0 0 680 300"
            className="w-full h-56 bg-slate-950/80 rounded-lg border border-slate-800"
            data-testid="geo-heatmap-svg"
          >
            {/* Equirectangular / Natural-Earth Graticule Lines */}
            {[60, 120, 180, 240].map((gy) => (
              <line
                key={`lat-${gy}`}
                x1="20"
                y1={gy}
                x2="660"
                y2={gy}
                stroke="#1e293b"
                strokeDasharray="3 3"
              />
            ))}
            {[140, 260, 380, 500].map((gx) => (
              <line
                key={`lon-${gx}`}
                x1={gx}
                y1="20"
                x2={gx}
                y2="280"
                stroke="#1e293b"
                strokeDasharray="3 3"
              />
            ))}

            {geoPoints.map((pt, idx) => {
              const { x, y } = projectCoords(pt.latitude, pt.longitude);
              const radius = Math.min(24, Math.max(6, Math.round(Math.sqrt(Number(pt.tx_count) || 1) * 1.8)));
              const fill = riskColor(pt.fraud_rate);

              return (
                <g
                  key={`${pt.country}-${pt.city}-${idx}`}
                  onMouseEnter={() => setHoveredPoint(pt)}
                  onMouseLeave={() => setHoveredPoint(null)}
                >
                  <circle
                    cx={x}
                    cy={y}
                    r={radius}
                    fill={fill}
                    fillOpacity="0.65"
                    stroke={fill}
                    strokeWidth="2"
                    data-testid="geo-point-circle"
                  />
                  <text
                    x={x}
                    y={y - radius - 4}
                    textAnchor="middle"
                    className="fill-slate-300 text-[9px] font-mono"
                  >
                    {pt.city} ({pt.country})
                  </text>
                </g>
              );
            })}
          </svg>

          {hoveredPoint && (
            <div
              className="mt-2 p-2.5 rounded bg-slate-900 border border-slate-700 text-xs flex items-center justify-between"
              data-testid="geo-tooltip"
            >
              <span className="font-semibold text-slate-100">
                {hoveredPoint.city}, {hoveredPoint.country}
              </span>
              <span>Tx Count: {hoveredPoint.tx_count}</span>
              <span>
                Volume: ${Number(hoveredPoint.total_amount || 0).toLocaleString("en-US")}
              </span>
              <span className="text-red-300 font-semibold">
                Fraud Rate: {(Number(hoveredPoint.fraud_rate || 0) * 100).toFixed(2)}%
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default GeoHeatmap;
