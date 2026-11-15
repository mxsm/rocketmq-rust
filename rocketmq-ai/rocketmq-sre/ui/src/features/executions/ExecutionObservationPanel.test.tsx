import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ExecutionObservationPanel } from "./ExecutionObservationPanel";
import { ExecutionTimeline } from "./ExecutionTimeline";
import { observationFixture } from "./test-fixtures/observation";

const preferences = vi.hoisted(() => ({ locale: "en-US" }));
vi.mock("@/preferences/OperatorPreferences", () => ({ useOperatorPreferences: () => ({
  ...preferences, formatDateTime: (value: string) => value,
}) }));

describe("execution observations remain distinct", () => {
  it("shows a recorded successful outcome and synthetic window without claiming business recovery", () => {
    const { view, verification } = observationFixture();
    view.binding.state = "succeeded";
    view.steps[0].verifications = [verification];
    render(<><ExecutionTimeline view={view} transport="sse" current /><ExecutionObservationPanel view={view} current /></>);
    const business = screen.getByRole("heading", { name: "Business recovery" }).closest("article")!;
    expect(within(business).getByText("Unknown")).toBeVisible();
    expect(within(business).getByText("Independent business-recovery observation is missing.")).toBeVisible();
    expect(screen.getByText("Successful execution state recorded")).toBeVisible();
    expect(screen.getByText("Synthetic engineering fixture")).toBeVisible();
    expect(screen.getByText("Valid historical window proof")).toBeVisible();
    expect(screen.getByText("Physical target version unknown")).toBeVisible();
    expect(screen.getByText(view.steps[0].target)).toBeVisible();
    expect(screen.queryByText("Physical version source-verified")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /execute|retry|compensate/i })).not.toBeInTheDocument();
  });

  it.each([
    { caseName: "credential rotation", action: "security.credential_rotate_overlap.v1", version: "1.0.0", locale: "en-US",
      target: "credential-set/CREDENTIAL_TARGET_DO_NOT_RENDER", hidden: "Target reference hidden for this action or version" },
    { caseName: "an unsupported descriptor version", action: "observability.logger_level_ttl.v1", version: "0.9.0", locale: "zh-CN",
      target: "broker/OLD_DESCRIPTOR_TARGET_DO_NOT_RENDER", hidden: "此动作或版本的目标引用已隐藏" },
    { caseName: "an action without a recognized display contract", action: "broker.container.add_remove.v1", version: "1.0.0", locale: "en-US",
      target: "broker/UNKNOWN_ACTION_TARGET_DO_NOT_RENDER", hidden: "Target reference hidden for this action or version" },
  ] as const)("hides the complete target for $caseName in $locale", ({ action, version, locale, target, hidden }) => {
    preferences.locale = locale;
    try {
      const { view } = observationFixture();
      view.steps[0] = { ...view.steps[0], action, descriptor_version: version, target };
      const { container } = render(<ExecutionObservationPanel view={view} current />);
      expect(screen.getByText(hidden)).toBeVisible();
      // Include attributes and hidden elements, not only visible text.
      expect(container.innerHTML).not.toContain(target);
    } finally { preferences.locale = "en-US"; }
  });

  it("renders only recorded events instead of checking off earlier phases", () => {
    const { view } = observationFixture();
    view.binding.state = "reconciling";
    render(<ExecutionTimeline view={view} transport="polling" current />);
    const list = screen.getByRole("list", { name: "Recorded execution timeline" });
    expect(within(list).getAllByRole("listitem")).toHaveLength(1);
    expect(within(list).getByText("9007199254740993")).toBeVisible();
    expect(within(list).getByText("Persisted intent")).toBeVisible();
    expect(within(list).queryByText("Recorded result")).not.toBeInTheDocument();
    const window = screen.getByRole("heading", { name: "Policy verification window" }).closest("article")!;
    expect(within(window).getByText("No verification window records were returned.")).toBeVisible();
    expect(within(window).queryByText("Intent is stored; target effects are not established by intent.")).not.toBeInTheDocument();
    expect(screen.queryByText(/Evidence.*sealed|evidence.*sealed/i)).not.toBeInTheDocument();
  });

  it("keeps legacy success readable while source and window facts remain missing", () => {
    const { view, verification } = observationFixture();
    view.steps[0].verifications = [{ ...verification, proof_state: "legacy_missing", provenance: "recorded_only",
      observed_stable_seconds: null, sample_count: null, source_first_at: null, source_last_at: null,
      resource_observed_at: null, collected_at: null, reason_codes: ["legacy_window_proof_missing"] }];
    render(<ExecutionObservationPanel view={view} current />);
    expect(screen.getByText("Legacy proof missing")).toBeVisible();
    expect(screen.getByText("Recorded history only")).toBeVisible();
    const duration = screen.getByText("Proven stable duration").parentElement!;
    expect(within(duration).getByText("Not supplied")).toBeVisible();
    const source = screen.getByText("Resource sample time").parentElement!;
    expect(within(source).getByText("Not supplied")).toBeVisible();
  });

  it("shows an unsafe numeric duration as unknown and hides unrecognized condition content", () => {
    const { view, verification } = observationFixture();
    view.steps[0].verifications = [{ ...verification, observed_stable_seconds: Number.MAX_SAFE_INTEGER + 1,
      missing_conditions: ["constructor"], reason_codes: ["source_stale", "observation_gap"] }];
    render(<ExecutionObservationPanel view={view} current />);
    expect(screen.getByText("No precisely readable value supplied")).toBeVisible();
    expect(screen.getByText("Unrecognized condition (raw value hidden)")).toBeVisible();
    expect(screen.queryByText("constructor")).not.toBeInTheDocument();
    expect(screen.getByText("Source data exceeds the policy age limit.")).toBeVisible();
    expect(screen.getByText("A sampling gap exceeds policy limits; stability is not established.")).toBeVisible();
  });

  it("does not keep current effect badges after the read snapshot expires", () => {
    const { view } = observationFixture();
    view.steps[0].effect = { state: "satisfied", source: "step_journal", reason_codes: ["result_recorded"] };
    render(<ExecutionObservationPanel view={view} current={false} />);
    expect(screen.queryByText("Observation satisfied")).not.toBeInTheDocument();
    expect(screen.getAllByText("Current observation is unconfirmed. Recorded history is retained.").length).toBeGreaterThan(0);
    expect(screen.getByText("9007199254740993")).toBeVisible();
  });

  it("uses the server business fact when independent observation is supplied", () => {
    preferences.locale = "zh-CN";
    try {
      const { view } = observationFixture();
      view.business_recovery = { state: "satisfied", source: "independent_business_observation", reason_codes: [] };
      render(<ExecutionTimeline view={view} transport="sse" current />);
      const business = screen.getByRole("heading", { name: "业务恢复" }).closest("article")!;
      expect(within(business).getByText("观测条件已满足")).toBeVisible();
      expect(within(business).getByText("来源 · 独立业务观测")).toBeVisible();
    } finally { preferences.locale = "en-US"; }
  });
});
