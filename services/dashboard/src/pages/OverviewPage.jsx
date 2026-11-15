import React from "react";
import MetricCard from "../components/MetricCard.jsx";
import AlertsTable from "../components/AlertsTable.jsx";
import TransactionVolume from "../components/charts/TransactionVolume.jsx";
import FraudRateGauge from "../components/charts/FraudRateGauge.jsx";
import SpendingCategories from "../components/charts/SpendingCategories.jsx";
import GeoHeatmap from "../components/charts/GeoHeatmap.jsx";
import AnomalyTimeline from "../components/charts/AnomalyTimeline.jsx";
import { acknowledgeAlert, dismissAlert, MOCK_FIXTURES } from "../services/api.js";

/**
 * Command OverviewPage (`STEP KUSHAGRA-MOD07-01` & `MOD07-02`)
 * Integrates KPI MetricCards, TransactionVolume, FraudRateGauge, GeoHeatmap,
 * AnomalyTimeline, SpendingCategories, and real-time AlertsTable.
 */
export function OverviewPage({
  overview = MOCK_FIXTURES.overview,
  volume = MOCK_FIXTURES.volume,
  categories = MOCK_FIXTURES.categories,
  geoPoints = MOCK_FIXTURES.geo,
  anomalies = MOCK_FIXTURES.anomalies,
  alerts = MOCK_FIXTURES.alerts,
  onAlertStatusChange = () => {},
  loading = false,
}) {
  async function handleAcknowledge(alertId) {
    const res = await acknowledgeAlert(alertId);
    onAlertStatusChange(alertId, res?.status || "acknowledged");
  }

  async function handleDismiss(alertId) {
    const res = await dismissAlert(alertId);
    onAlertStatusChange(alertId, res?.status || "dismissed");
  }

  const sparklines = overview?.sparklines || {};

  return (
    <div className="space-y-4" data-testid="overview-page">
      {/* Row 1: 5 Core KPI MetricCards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        <MetricCard
          title="Current Throughput"
          value={Number(overview?.current_tps || 0).toFixed(2)}
          unit="TPS"
          delta={4.2}
          sparklineData={sparklines.tps || []}
          status="normal"
        />
        <MetricCard
          title="Total Volume (24h)"
          value={`$${Number(overview?.total_volume_usd || 0).toLocaleString("en-US")}`}
          unit="USD"
          delta={8.5}
          sparklineData={sparklines.volume || []}
          status="normal"
        />
        <MetricCard
          title="Fraud Rate"
          value={`${(Number(overview?.fraud_rate || 0) * 100).toFixed(2)}%`}
          delta={-0.3}
          sparklineData={sparklines.fraud_rate || []}
          status={Number(overview?.fraud_rate || 0) > 0.05 ? "critical" : "warning"}
        />
        <MetricCard
          title="Active Fraud Alerts"
          value={overview?.active_alerts_count ?? alerts.length}
          unit="alerts"
          status="critical"
        />
        <MetricCard
          title="P95 Scoring Latency"
          value={Number(overview?.p95_latency_ms || 18.4).toFixed(1)}
          unit="ms"
          status="normal"
        />
      </div>

      {/* Row 2: TransactionVolume + FraudRateGauge */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <TransactionVolume data={volume} loading={loading} />
        </div>
        <FraudRateGauge
          fraudRate={overview?.fraud_rate || 0}
          flaggedCount={overview?.flagged_count || 0}
          blockedCount={overview?.blocked_count || 0}
          totalCount={overview?.total_transactions || 0}
        />
      </div>

      {/* Row 3: GeoHeatmap + AnomalyTimeline */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <GeoHeatmap geoPoints={geoPoints} loading={loading} />
        <AnomalyTimeline anomalies={anomalies} loading={loading} />
      </div>

      {/* Row 4: SpendingCategories + Live AlertsTable */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <SpendingCategories categories={categories} loading={loading} />
        <AlertsTable
          alerts={alerts}
          onAcknowledge={handleAcknowledge}
          onDismiss={handleDismiss}
        />
      </div>
    </div>
  );
}

export default OverviewPage;
