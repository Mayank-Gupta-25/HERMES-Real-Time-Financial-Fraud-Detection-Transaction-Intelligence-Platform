import React from "react";
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import SpendingCategories from "../components/charts/SpendingCategories.jsx";
import CustomerIntelPage from "../pages/CustomerIntelPage.jsx";
import App from "../App.jsx";
import { MOCK_FIXTURES } from "../services/api.js";

describe("TC-30 / TC-32: SpendingCategories, CustomerIntelPage & Full 4-Page App Navigation", () => {
  it("renders SpendingCategories breakdown rows", () => {
    render(<SpendingCategories categories={MOCK_FIXTURES.categories} loading={false} />);
    expect(screen.getByTestId("spending-categories-chart")).toBeDefined();
    expect(screen.getByTestId("category-row-money_transfer")).toBeDefined();
  });

  it("renders CustomerIntelPage with Customer 360 profile, Welford baseline, AML watchlist, and Parquet archive", () => {
    render(<CustomerIntelPage />);
    expect(screen.getByTestId("customer-intel-page")).toBeDefined();
    expect(screen.getByTestId("customer-360-card")).toBeDefined();
    expect(screen.getByTestId("aml-watchlist-panel")).toBeDefined();
    expect(screen.getByTestId("parquet-archive-panel")).toBeDefined();
  });

  it("navigates across all 4 pages in unified App router", () => {
    render(<App />);
    expect(screen.getByTestId("overview-page")).toBeDefined();

    fireEvent.click(screen.getByTestId("nav-transactions"));
    expect(screen.getByTestId("transactions-page")).toBeDefined();

    fireEvent.click(screen.getByTestId("nav-fraud-analytics"));
    expect(screen.getByTestId("fraud-analytics-page")).toBeDefined();

    fireEvent.click(screen.getByTestId("nav-customer-intel"));
    expect(screen.getByTestId("customer-intel-page")).toBeDefined();
  });
});
