import React, { useState } from "react";
import Navbar from "./components/Navbar.jsx";
import Sidebar from "./components/Sidebar.jsx";
import OverviewPage from "./pages/OverviewPage.jsx";
import TransactionsPage from "./pages/TransactionsPage.jsx";
import FraudAnalyticsPage from "./pages/FraudAnalyticsPage.jsx";
import CustomerIntelPage from "./pages/CustomerIntelPage.jsx";
import useMetrics from "./hooks/useMetrics.js";
import useWebSocket from "./hooks/useWebSocket.js";

/**
 * Unified 4-Page HERMES Real-Time Command Center (`STEP KUSHAGRA-MOD07-01` & `MOD07-02`)
 * Connects OverviewPage, TransactionsPage, FraudAnalyticsPage, and CustomerIntelPage.
 */
export function App({ initialPage = "overview" }) {
  const [activePage, setActivePage] = useState(initialPage);
  const [selectedRange, setSelectedRange] = useState("24h");

  const {
    overview,
    volume,
    categories,
    geoPoints,
    anomalies,
    alerts: apiAlerts,
    loading,
  } = useMetrics(selectedRange, 0);

  const { status: wsStatus, alerts: wsAlerts, latestToast, clearToast } = useWebSocket(
    undefined,
    apiAlerts
  );

  const mergedAlerts = wsAlerts && wsAlerts.length > 0 ? wsAlerts : apiAlerts;

  function renderActivePage() {
    switch (activePage) {
      case "transactions":
      case "/transactions":
        return <TransactionsPage initialAlerts={mergedAlerts} />;
      case "fraud-analytics":
      case "/fraud-analytics":
        return <FraudAnalyticsPage />;
      case "customer-intel":
      case "/customer-intel":
        return <CustomerIntelPage />;
      case "overview":
      case "/":
      default:
        return (
          <OverviewPage
            overview={overview}
            volume={volume}
            categories={categories}
            geoPoints={geoPoints}
            anomalies={anomalies}
            alerts={mergedAlerts}
            loading={loading}
          />
        );
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-4 flex gap-4" data-testid="hermes-app">
      <Sidebar
        activePage={activePage}
        onNavigate={(pageId) => setActivePage(pageId)}
      />

      <div className="flex-1 flex flex-col min-w-0">
        <Navbar
          wsStatus={wsStatus}
          selectedRange={selectedRange}
          onRangeChange={(nextRange) => setSelectedRange(nextRange)}
          activeAlertCount={mergedAlerts.filter((a) => a.status === "active").length}
        />

        {latestToast && (
          <div
            className="mb-4 p-3 rounded-lg bg-red-950/90 border border-red-700 flex items-center justify-between text-xs"
            data-testid="ws-alert-toast"
          >
            <div>
              <span className="font-bold text-red-300 uppercase mr-2">
                LIVE {latestToast.risk_band || "CRITICAL"} FRAUD ALERT:
              </span>
              <span className="font-mono text-slate-100">
                {latestToast.alert_id || latestToast.event_id} · ${latestToast.amount}
              </span>
            </div>
            <button
              type="button"
              onClick={clearToast}
              className="px-2 py-0.5 rounded bg-red-900 text-red-100"
            >
              Dismiss
            </button>
          </div>
        )}

        <main className="flex-1">{renderActivePage()}</main>
      </div>
    </div>
  );
}

export default App;
