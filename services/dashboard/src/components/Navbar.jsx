import React from "react";

export function Navbar({
  wsStatus = "CONNECTED",
  selectedRange = "24h",
  onRangeChange = () => {},
  activeAlertCount = 0,
}) {
  const isConnected = wsStatus === "CONNECTED";
  const ranges = ["1h", "24h", "7d", "30d"];

  return (
    <header
      className="glass-panel flex items-center justify-between px-6 py-3 mb-4 border-b border-slate-800"
      data-testid="hermes-navbar"
    >
      <div className="flex items-center gap-3">
        <div className="h-3 w-3 rounded-full bg-blue-500 animate-pulse" />
        <span className="font-bold tracking-wider text-slate-100 text-lg">
          HERMES COMMAND CENTER
        </span>
        <span className="text-xs px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700">
          Kappa Stream · Polyglot Federation
        </span>
      </div>

      <div className="flex items-center gap-4">
        <div
          className="flex items-center gap-1 bg-slate-900/90 p-1 rounded-lg border border-slate-800"
          role="group"
          aria-label="Time Range Selector"
        >
          {ranges.map((r) => (
            <button
              key={r}
              type="button"
              onClick={() => onRangeChange(r)}
              className={`px-2.5 py-1 text-xs font-medium rounded-md transition ${
                selectedRange === r
                  ? "bg-blue-600 text-white"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              {r}
            </button>
          ))}
        </div>

        <div
          className="flex items-center gap-2 text-xs px-3 py-1.5 rounded-full border border-slate-800 bg-slate-900"
          data-testid="ws-status-pill"
        >
          <span
            className={`h-2 w-2 rounded-full ${
              isConnected ? "bg-emerald-400" : "bg-amber-400 animate-ping"
            }`}
          />
          <span className="font-medium text-slate-300">{wsStatus}</span>
        </div>

        <div className="text-xs px-3 py-1.5 rounded-full bg-red-950/60 border border-red-800/60 text-red-300 font-semibold">
          Active Alerts: {activeAlertCount}
        </div>
      </div>
    </header>
  );
}

export default Navbar;
