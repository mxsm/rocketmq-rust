import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { TypedActionForm } from "./TypedActionForm";
import { buildActionParameters, initialActionDraft } from "./actionParameters";

beforeEach(() => window.localStorage.clear());
function Fixture() {
  const [value, setValue] = useState(initialActionDraft("observability.logger_level_ttl.v1", "broker/127.0.0.1:10911"));
  return <OperatorPreferencesProvider>
    <TypedActionForm value={value} onChange={(change) => setValue((old) => ({ ...old, ...change }))}
      onActionChange={(id) => setValue(initialActionDraft(id))} />
    <output aria-label="synthetic payload">{JSON.stringify(buildActionParameters(value) ?? null)}</output>
  </OperatorPreferencesProvider>;
}

describe("typed action fields", () => {
  it("offers exactly the nine wired forms without any generic command or JSON editor", async () => {
    render(<Fixture />);
    expect((screen.getByLabelText("动作类型") as HTMLSelectElement).options).toHaveLength(9);
    expect(screen.queryByRole("textbox", { name: /JSON|shell|command/i })).not.toBeInTheDocument();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByLabelText("动作类型"), "proxy.restart_one.v1");
    expect(screen.getByLabelText("命名空间")).toHaveValue("");
    expect(screen.getByLabelText("Pod")).toHaveValue("");
    expect(screen.getByLabelText("预期 Pod UID")).toHaveValue("");
    expect(screen.queryByLabelText("Logger")).not.toBeInTheDocument();
  });

  it("requires an explicit field selection and omits unselected patch values", async () => {
    render(<Fixture />);
    const user = userEvent.setup();
    await user.selectOptions(screen.getByLabelText("动作类型"), "broker.config.patch_allowlisted.v1");
    fireEvent.change(screen.getByLabelText("Broker 地址选择器"), { target: { value: "127.0.0.1:10911" } });
    fireEvent.change(screen.getByLabelText("预期配置代次"), { target: { value: "4" } });
    expect(screen.getByLabelText("发送线程数")).toBeDisabled();
    expect(screen.getByLabelText("synthetic payload")).toHaveTextContent("null");
    await user.click(screen.getByLabelText("变更 发送线程数"));
    fireEvent.change(screen.getByLabelText("发送线程数"), { target: { value: "16" } });
    expect(JSON.parse(screen.getByLabelText("synthetic payload").textContent!)).toEqual({
      broker: "127.0.0.1:10911", expected_generation: 4, patch: { send_message_thread_pool_nums: 16 },
    });
    await user.click(screen.getByLabelText("变更 发送线程数"));
    expect(screen.getByLabelText("synthetic payload")).toHaveTextContent("null");
  });

  it("masks managed reference entry and fixes the canary count to one", async () => {
    render(<Fixture />);
    const user = userEvent.setup();
    await user.selectOptions(screen.getByLabelText("动作类型"), "security.credential_rotate_overlap.v1");
    expect(screen.getByLabelText("受管凭据引用")).toHaveAttribute("type", "password");
    expect(screen.getByText(/仅填写 vault/)).toBeVisible();
    await user.selectOptions(screen.getByLabelText("动作类型"), "proxy.rollout_image_canary.v1");
    expect(screen.getByLabelText("金丝雀副本数")).toHaveValue(1);
    expect(screen.getByLabelText("金丝雀副本数")).toHaveAttribute("readonly");
  });
});
