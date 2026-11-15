import React from "react";
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import MetricCard from "../components/MetricCard.jsx";
import TransactionVolume from "../components/charts/TransactionVolume.jsx";
import FraudRateGauge from "../components/charts/FraudRateGauge.jsx";
import OverviewPage from "../pages/OverviewPage.jsx";
import { MOCK_FIXTURES } from "../services/api.js";

describe("TC-30: OverviewPage & KPI Charts", () => {
  it("renders MetricCard title, value, unit, delta, and sparkline", () => {
    render(
      <MetricCard
        title="Current Throughput"
        value="12.50"
        unit="TPS"
        delta={5.2}
        sparklineData={MOCK_FIXTURES.overview.sparklines.tps}
        status="normal"
      />
    );
    expect(screen.getByText("Current Throughput")).toBeDefined();
    expect(screen.getByTestId("metric-card-value").textContent).toBe("12.50");
    expect(screen.getByTestId("metric-card-sparkline")).toBeDefined();
  });

  it("renders TransactionVolume SVG bars and FraudRateGauge split", () => {
    render(<TransactionVolume data={MOCK_FIXTURES.volume} loading={false} />);
    expect(screen.getAllByTestId("volume-bar").length).toBe(MOCK_FIXTURES.volume.length);

    render(
      <FraudRateGauge
        fraudRate={0.042}
        flaggedCount={14}
        blockedCount={7}
        totalCount={500}
      />
    );
    expect(screen.getByTestId("gauge-rate-pct").textContent).toBe("4.2%");
    expect(screen.getByTestId("gauge-flagged-count").textContent).toBe("14");
    expect(screen.getByTestId("gauge-blocked-count").textContent).toBe("7");
  });

  it("renders full OverviewPage with all KPI cards and embedded visual panels", () => {
    render(<OverviewPage />);
    expect(screen.getByTestId("overview-page")).toBeDefined();
    expect(screen.getAllByTestId("metric-card").length).toBe(5);
    expect(screen.getByTestId("transaction-volume-chart")).toBeDefined();
    expect(screen.getByTestId("fraud-rate-gauge")).toBeDefined();
    expect(screen.getByTestId("geo-heatmap-chart")).toBeDefined();
    expect(screen.getByTestId("anomaly-timeline-chart")).toBeDefined();
  });
});
