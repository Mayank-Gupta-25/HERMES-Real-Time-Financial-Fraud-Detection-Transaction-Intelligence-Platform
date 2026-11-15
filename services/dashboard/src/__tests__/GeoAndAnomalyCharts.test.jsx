import React from "react";
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import GeoHeatmap from "../components/charts/GeoHeatmap.jsx";
import AnomalyTimeline from "../components/charts/AnomalyTimeline.jsx";
import { MOCK_FIXTURES } from "../services/api.js";

describe("TC-30: GeoHeatmap & AnomalyTimeline D3/SVG Components", () => {
  it("mounts GeoHeatmap with sample geoPoints and handles empty/loading states", () => {
    const { rerender } = render(
      <GeoHeatmap geoPoints={MOCK_FIXTURES.geo} loading={false} />
    );
    expect(screen.getByTestId("geo-heatmap-svg")).toBeDefined();
    expect(screen.getAllByTestId("geo-point-circle").length).toBe(
      MOCK_FIXTURES.geo.length
    );

    rerender(<GeoHeatmap geoPoints={[]} loading={false} />);
    expect(screen.getByTestId("geo-empty-state")).toBeDefined();

    rerender(<GeoHeatmap geoPoints={[]} loading={true} />);
    expect(screen.getByTestId("geo-heatmap-loading")).toBeDefined();
  });

  it("mounts AnomalyTimeline with sample anomalies and handles empty/loading states", () => {
    const { rerender } = render(
      <AnomalyTimeline anomalies={MOCK_FIXTURES.anomalies} loading={false} />
    );
    expect(screen.getByTestId("anomaly-timeline-svg")).toBeDefined();
    expect(screen.getAllByTestId("anomaly-point-circle").length).toBe(
      MOCK_FIXTURES.anomalies.length
    );

    rerender(<AnomalyTimeline anomalies={[]} loading={false} />);
    expect(screen.getByTestId("anomaly-empty-state")).toBeDefined();

    rerender(<AnomalyTimeline anomalies={[]} loading={true} />);
    expect(screen.getByTestId("anomaly-timeline-loading")).toBeDefined();
  });
});
