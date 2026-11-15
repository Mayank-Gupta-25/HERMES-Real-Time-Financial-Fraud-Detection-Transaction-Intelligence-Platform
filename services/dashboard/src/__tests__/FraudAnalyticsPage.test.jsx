import React from "react";
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import FraudAnalyticsPage from "../pages/FraudAnalyticsPage.jsx";

describe("TC-30 / TC-31: FraudAnalyticsPage & Model Metrics UI", () => {
  it("renders ROC/PR KPIs, YAML rule trigger frequencies, score distribution, and shadow A/B stats", () => {
    render(<FraudAnalyticsPage />);
    expect(screen.getByTestId("fraud-analytics-page")).toBeDefined();
    expect(screen.getByTestId("rule-trigger-panel")).toBeDefined();
    expect(screen.getByTestId("score-distribution-panel")).toBeDefined();
    expect(screen.getByTestId("shadow-comparison-box")).toBeDefined();
    expect(screen.getByTestId("rule-freq-GEO_IMPOSSIBILITY")).toBeDefined();
  });
});
