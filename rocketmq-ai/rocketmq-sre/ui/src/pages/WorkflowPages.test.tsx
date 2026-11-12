import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { AskSrePage } from "./WorkflowPages";

const A = "10000000-0000-4000-8000-000000000001";
const B = "10000000-0000-4000-8000-000000000002";
const api = vi.hoisted(() => ({
  createConversation:
    vi.fn<(input: unknown, signal?: AbortSignal) => Promise<unknown>>(),
  listConversations: vi.fn(async () => ({
    items: [],
    partial: false,
    warnings: [],
    observed_at: "2026-11-11T10:00:00Z",
  })),
}));

vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({
    api,
    clusters: [
      {
        id: "10000000-0000-4000-8000-000000000001",
        external_cluster_key: "cluster-a",
        environment: "test",
      },
      {
        id: "10000000-0000-4000-8000-000000000002",
        external_cluster_key: "cluster-b",
        environment: "test",
      },
    ],
  }),
}));
vi.mock("@/hooks/useWorkflowProgress", () => ({
  useWorkflowProgress: () => ({ transport: "connecting", events: [] }),
}));

describe("legacy workflow request ownership", () => {
  it("does not navigate back to a created conversation after the old scope has unmounted", async () => {
    const user = userEvent.setup();
    let complete!: (value: unknown) => void;
    api.createConversation.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    render(
      <MemoryRouter initialEntries={[`/ask?cluster_id=${A}`]}>
        <WorkflowHarness />
      </MemoryRouter>,
    );
    await user.type(
      screen.getByLabelText("问题"),
      "Why is the consumer lagging?",
    );
    await user.click(screen.getByRole("button", { name: "开始只读调查" }));
    await waitFor(() => expect(api.createConversation).toHaveBeenCalledOnce());

    await user.click(
      screen.getByRole("button", { name: "Switch global scope" }),
    );
    await act(async () =>
      complete({ conversation: { id: "old-conversation" } }),
    );

    expect(screen.getByTestId("location")).toHaveTextContent(
      `/investigations?cluster_id=${B}`,
    );
    expect(screen.getByTestId("location")).not.toHaveTextContent(
      "old-conversation",
    );
    expect(api.createConversation.mock.calls[0][1]?.aborted).toBe(true);
  });
});

function WorkflowHarness() {
  const [active, setActive] = useState(true);
  const navigate = useNavigate();
  const location = useLocation();
  return (
    <>
      <button
        onClick={() => {
          setActive(false);
          navigate(`/investigations?cluster_id=${B}`);
        }}
        type="button"
      >
        Switch global scope
      </button>
      {active && <AskSrePage />}
      <output data-testid="location">
        {location.pathname}
        {location.search}
      </output>
    </>
  );
}
