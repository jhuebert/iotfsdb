import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { CreateSeriesDialog } from "./create-series-dialog";

vi.mock("@/api/series", () => ({
  createSeries: vi.fn().mockResolvedValue(undefined),
  getUiConfig: vi.fn().mockResolvedValue({
    version: "test",
    readOnly: false,
    springdocEnabled: false,
    statsEnabled: false,
    maxQuerySize: 1000,
    defaultSeries: {
      definition: { id: "default", type: "FLOAT4", interval: 60000, partition: "MONTH" },
      metadata: {},
    },
  }),
}));

import { createSeries } from "@/api/series";

beforeEach(() => {
  vi.clearAllMocks();
});

function renderDialog() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  });
  const onOpenChange = vi.fn();
  render(
    <QueryClientProvider client={queryClient}>
      <CreateSeriesDialog open onOpenChange={onOpenChange} />
    </QueryClientProvider>,
  );
  return { onOpenChange };
}

describe("CreateSeriesDialog", () => {
  it("validates the series id", async () => {
    const user = userEvent.setup();
    renderDialog();

    await user.type(screen.getByLabelText("ID"), "Bad ID!");
    await user.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByText(/Lowercase letters/)).toBeInTheDocument();
    expect(createSeries).not.toHaveBeenCalled();
  });

  it("submits a valid definition", async () => {
    const user = userEvent.setup();
    const { onOpenChange } = renderDialog();

    await user.type(screen.getByLabelText("ID"), "temp1");
    await user.click(screen.getByRole("button", { name: "Create" }));

    await vi.waitFor(() => expect(createSeries).toHaveBeenCalledTimes(1));
    expect(createSeries).toHaveBeenCalledWith({
      definition: {
        id: "temp1",
        type: "FLOAT4",
        interval: 60000,
        partition: "MONTH",
      },
      metadata: {},
    });
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it("rejects min without max", async () => {
    const user = userEvent.setup();
    renderDialog();

    await user.type(screen.getByLabelText("ID"), "temp2");
    await user.click(screen.getByLabelText("Number Type"));
    await user.click(await screen.findByRole("option", { name: "MAPPED4" }));
    await user.type(screen.getByLabelText("Minimum"), "10");
    await user.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByText(/must be provided together/)).toBeInTheDocument();
    expect(createSeries).not.toHaveBeenCalled();
  });
});
