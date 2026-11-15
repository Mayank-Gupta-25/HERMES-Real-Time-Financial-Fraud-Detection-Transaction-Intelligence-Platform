import React from "react";

const NAV_ITEMS = [
  { id: "overview", path: "/", label: "Command Overview", badge: "LIVE" },
  { id: "transactions", path: "/transactions", label: "Transactions & Alerts", badge: "OPS" },
  { id: "fraud-analytics", path: "/fraud-analytics", label: "Fraud & ML Analytics", badge: "XGB" },
  { id: "customer-intel", path: "/customer-intel", label: "Customer 360 & AML", badge: "AML" },
];

export function Sidebar({ activePage = "overview", onNavigate = () => {} }) {
  return (
    <aside
      className="glass-panel w-64 shrink-0 p-4 flex flex-col justify-between min-h-[calc(100vh-2rem)]"
      data-testid="hermes-sidebar"
    >
      <div>
        <div className="mb-6 px-2">
          <div className="text-xs uppercase tracking-widest text-blue-400 font-semibold">
            Real-Time Intelligence
          </div>
          <div className="text-xl font-extrabold text-white mt-0.5">HERMES v1.0</div>
        </div>

        <nav className="space-y-1.5" aria-label="Primary Navigation">
          {NAV_ITEMS.map((item) => {
            const isActive = activePage === item.id || activePage === item.path;
            return (
              <button
                key={item.id}
                type="button"
                onClick={() => onNavigate(item.id, item.path)}
                data-testid={`nav-${item.id}`}
                className={`w-full flex items-center justify-between px-3 py-2.5 rounded-lg text-sm font-medium transition ${
                  isActive
                    ? "bg-blue-600/25 text-blue-300 border border-blue-500/40"
                    : "text-slate-400 hover:bg-slate-800/60 hover:text-slate-200"
                }`}
              >
                <span>{item.label}</span>
                <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-300">
                  {item.badge}
                </span>
              </button>
            );
          })}
        </nav>
      </div>

      <div className="p-3 rounded-lg bg-slate-900/80 border border-slate-800 text-xs text-slate-400 space-y-1">
        <div className="font-semibold text-slate-300">Polyglot Stores</div>
        <div>PostgreSQL · Operational</div>
        <div>TimescaleDB · Rollups</div>
        <div>Redis 7 · Hot Baselines</div>
        <div>MinIO S3 · Cold Parquet</div>
      </div>
    </aside>
  );
}

export default Sidebar;
