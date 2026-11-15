import React, { useEffect, useState } from "react";
import TransactionTable from "../components/TransactionTable.jsx";
import AlertsTable from "../components/AlertsTable.jsx";
import {
  acknowledgeAlert,
  dismissAlert,
  getAlerts,
  getTransactions,
  MOCK_FIXTURES,
} from "../services/api.js";

/**
 * Operational Transactions & Alerts Explorer Page (`STEP MAYANK-MOD07-01`)
 */
export function TransactionsPage({ initialAlerts = null }) {
  const [txData, setTxData] = useState(MOCK_FIXTURES.transactions);
  const [alerts, setAlerts] = useState(initialAlerts || MOCK_FIXTURES.alerts);
  const [page, setPage] = useState(1);
  const [filters, setFilters] = useState({});
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const [txRes, alRes] = await Promise.all([
          getTransactions({ page, page_size: 20, ...filters }),
          getAlerts("all", 50),
        ]);
        if (!cancelled) {
          if (txRes && Array.isArray(txRes.items)) setTxData(txRes);
          if (Array.isArray(alRes)) setAlerts(alRes);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [page, filters]);

  async function handleAcknowledge(alertId) {
    await acknowledgeAlert(alertId);
    setAlerts((prev) =>
      prev.map((a) => (a.alert_id === alertId ? { ...a, status: "acknowledged" } : a))
    );
  }

  async function handleDismiss(alertId) {
    await dismissAlert(alertId);
    setAlerts((prev) =>
      prev.map((a) => (a.alert_id === alertId ? { ...a, status: "dismissed" } : a))
    );
  }

  function exportCsv() {
    const items = txData?.items || [];
    const header = "event_id,event_time,customer_id,category,amount,fraud_score,risk_band,decision\n";
    const rows = items
      .map(
        (t) =>
          `${t.event_id},${t.event_time},${t.customer_id},${t.category},${t.amount},${t.fraud_score},${t.risk_band},${t.decision}`
      )
      .join("\n");
    return header + rows;
  }

  return (
    <div className="space-y-4" data-testid="transactions-page">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-bold text-slate-100">
            Operational Transaction & Alert Explorer
          </h2>
          <p className="text-xs text-slate-400">
            PostgreSQL B-Tree Indexed Explorer · Full Per-Event Provenance & Alert Management
          </p>
        </div>
        <button
          type="button"
          onClick={exportCsv}
          data-testid="export-csv-btn"
          className="px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-xs font-medium text-white"
        >
          Export Filtered CSV
        </button>
      </div>

      <AlertsTable
        alerts={alerts}
        onAcknowledge={handleAcknowledge}
        onDismiss={handleDismiss}
      />

      <TransactionTable
        transactions={txData?.items || []}
        totalCount={txData?.total || 0}
        page={page}
        pageSize={txData?.page_size || 20}
        onPageChange={(nextPage) => setPage(nextPage)}
        onFilterChange={(nextFilters) => {
          setPage(1);
          setFilters(nextFilters);
        }}
        loading={loading}
      />
    </div>
  );
}

export default TransactionsPage;
