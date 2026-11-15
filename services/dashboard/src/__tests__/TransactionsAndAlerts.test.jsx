import React from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import TransactionTable from "../components/TransactionTable.jsx";
import AlertsTable from "../components/AlertsTable.jsx";
import TransactionsPage from "../pages/TransactionsPage.jsx";
import { MOCK_FIXTURES } from "../services/api.js";

describe("TC-30: TransactionTable, AlertsTable & TransactionsPage", () => {
  it("renders TransactionTable rows, risk badges, and expands audit provenance drawer", () => {
    const sampleTx = MOCK_FIXTURES.transactions.items[0];
    render(
      <TransactionTable
        transactions={[sampleTx]}
        totalCount={1}
        page={1}
        pageSize={20}
      />
    );

    const row = screen.getByTestId(`tx-row-${sampleTx.event_id}`);
    expect(row).toBeDefined();
    expect(screen.getByTestId(`risk-badge-${sampleTx.event_id}`).textContent).toBe("critical");

    // Click row to expand provenance drawer
    fireEvent.click(row);
    const drawer = screen.getByTestId(`provenance-drawer-${sampleTx.event_id}`);
    expect(drawer).toBeDefined();
    expect(drawer.textContent).toContain("hermes.transactions.raw");
    expect(drawer.textContent).toContain("travel_speed_kmph");
  });

  it("renders AlertsTable and triggers onAcknowledge and onDismiss callbacks", () => {
    const sampleAlert = MOCK_FIXTURES.alerts[0];
    const onAck = vi.fn();
    const onDismiss = vi.fn();

    render(
      <AlertsTable
        alerts={[sampleAlert]}
        onAcknowledge={onAck}
        onDismiss={onDismiss}
      />
    );

    fireEvent.click(screen.getByTestId(`ack-btn-${sampleAlert.alert_id}`));
    expect(onAck).toHaveBeenCalledWith(sampleAlert.alert_id);

    fireEvent.click(screen.getByTestId(`dismiss-btn-${sampleAlert.alert_id}`));
    expect(onDismiss).toHaveBeenCalledWith(sampleAlert.alert_id);
  });

  it("renders TransactionsPage explorer with CSV export button", () => {
    render(<TransactionsPage />);
    expect(screen.getByTestId("transactions-page")).toBeDefined();
    expect(screen.getByTestId("export-csv-btn")).toBeDefined();
  });
});
