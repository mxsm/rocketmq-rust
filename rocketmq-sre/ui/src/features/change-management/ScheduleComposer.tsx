import {
  FileCheck2,
  Link2,
  Plus,
  RotateCcw,
  ShieldCheck,
} from "lucide-react";
import type { FormEvent } from "react";
import { useEffect, useMemo, useState } from "react";

import { createChangeManagementApi } from "@/api/changeManagementClient";
import type {
  ChangeSchedule,
  ChangeSchedulePreview,
  CreateChangeScheduleRequest,
  RunbookDefinition,
  RunbookStepPlanBinding,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import { ConflictPanel } from "./ChangeWorkspace";
import {
  dateTimeLocalToIso,
  toDateTimeLocal,
} from "./changePresentation";
import { createMockChangeManagementApi } from "@/data/phase3ChangeDemo";

interface BindingDraft {
  stepId: string;
  stepName: string;
  action: string;
  planId: string;
  planHash: string;
  preconditionHash: string;
}

export function ScheduleComposer({
  clusterId,
  runbooks,
  canCreate,
  demoMode,
  onCreated,
}: {
  clusterId: string;
  runbooks: RunbookDefinition[];
  canCreate: boolean;
  demoMode: boolean;
  onCreated: (schedule: ChangeSchedule) => void;
}) {
  const auth = useAuth();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const defaults = useMemo(scheduleDefaults, []);
  const [selection, setSelection] = useState("");
  const [startsAt, setStartsAt] = useState(defaults.startsAt);
  const [endsAt, setEndsAt] = useState(defaults.endsAt);
  const [bindings, setBindings] = useState<BindingDraft[]>([]);
  const [preview, setPreview] = useState<ChangeSchedulePreview>();
  const [busy, setBusy] = useState<"preview" | "create">();
  const [message, setMessage] = useState<string>();

  const selected = runbooks.find(
    (runbook) => runbookKey(runbook) === selection,
  );

  useEffect(() => {
    if (!selection && runbooks[0]) {
      setSelection(runbookKey(runbooks[0]));
    }
  }, [runbooks, selection]);

  useEffect(() => {
    setPreview(undefined);
    setBindings((current) => {
      if (!selected) {
        return [];
      }
      return selected.steps
        .filter((step) => step.body.kind === "action")
        .map((step) => {
          const previous = current.find((item) => item.stepId === step.id);
          return {
            stepId: step.id,
            stepName: step.name,
            action:
              step.body.kind === "action" ? step.body.action : "manual_gate",
            planId: previous?.planId ?? "",
            planHash: previous?.planHash ?? "",
            preconditionHash: previous?.preconditionHash ?? "",
          };
        });
    });
  }, [selected]);

  const updateBinding = (
    stepId: string,
    field: "planId" | "planHash" | "preconditionHash",
    value: string,
  ) => {
    setPreview(undefined);
    setBindings((current) =>
      current.map((binding) =>
        binding.stepId === stepId
          ? { ...binding, [field]: value }
          : binding,
      ),
    );
  };

  const request = (): CreateChangeScheduleRequest | undefined => {
    if (
      !selected ||
      bindings.some(
        (binding) =>
          !binding.planId.trim() ||
          !binding.planHash.trim() ||
          !binding.preconditionHash.trim(),
      )
    ) {
      setMessage("每个动作步骤都必须绑定已批准 Plan 及两个摘要。");
      return undefined;
    }
    return {
      cluster_id: clusterId,
      runbook_id: selected.id,
      runbook_version: selected.version,
      scheduled_start: dateTimeLocalToIso(startsAt),
      scheduled_end: dateTimeLocalToIso(endsAt),
      plan_bindings: bindings.map(
        (binding): RunbookStepPlanBinding => ({
          step_id: binding.stepId,
          plan_id: binding.planId.trim(),
          plan_hash: binding.planHash.trim(),
          precondition_hash: binding.preconditionHash.trim(),
        }),
      ),
    };
  };

  const previewSchedule = async (event: FormEvent) => {
    event.preventDefault();
    if (!api || !clusterId) {
      return;
    }
    const input = request();
    if (!input) {
      return;
    }
    setBusy("preview");
    setMessage(undefined);
    try {
      setPreview(await api.previewSchedule(input));
    } catch (cause) {
      setPreview(undefined);
      setMessage(
        cause instanceof Error ? cause.message : "排程预演失败",
      );
    } finally {
      setBusy(undefined);
    }
  };

  const createSchedule = async () => {
    if (!api) {
      return;
    }
    const input = request();
    if (!input) {
      return;
    }
    setBusy("create");
    setMessage(undefined);
    try {
      const created = await api.createSchedule(input);
      setPreview(undefined);
      setMessage("排程已创建；服务端已再次检查窗口与冲突。");
      onCreated(created);
    } catch (cause) {
      setMessage(
        cause instanceof Error ? cause.message : "排程创建失败",
      );
    } finally {
      setBusy(undefined);
    }
  };

  return (
    <aside className="schedule-composer">
      <header>
        <span className="composer-icon">
          <Plus aria-hidden="true" size={18} />
        </span>
        <div>
          <span className="section-kicker">SCHEDULE COMPOSER</span>
          <h2>创建受控排程</h2>
          <p>先预演，再创建；两次都由服务端执行同一冲突校验。</p>
        </div>
      </header>

      <form className="change-form" onSubmit={(event) => void previewSchedule(event)}>
        <label className="span-2">
          <span>Runbook 版本</span>
          <Select
            disabled={!canCreate}
            value={selection}
            onValueChange={setSelection}
          >
            <SelectTrigger aria-label="Runbook 版本">
              <SelectValue placeholder="选择 Runbook" />
            </SelectTrigger>
            <SelectContent>
              {runbooks.map((runbook) => (
                <SelectItem key={runbookKey(runbook)} value={runbookKey(runbook)}>
                  {runbook.name} · {runbook.version} · {runbook.risk.toUpperCase()}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <label>
          <span>计划开始</span>
          <input
            disabled={!canCreate}
            onChange={(event) => {
              setStartsAt(event.target.value);
              setPreview(undefined);
            }}
            type="datetime-local"
            value={startsAt}
          />
        </label>
        <label>
          <span>计划结束</span>
          <input
            disabled={!canCreate}
            onChange={(event) => {
              setEndsAt(event.target.value);
              setPreview(undefined);
            }}
            type="datetime-local"
            value={endsAt}
          />
        </label>

        <section className="plan-binding-editor span-2">
          <header>
            <div>
              <Link2 aria-hidden="true" size={16} />
              <strong>已批准 Plan 绑定</strong>
            </div>
            <Badge variant="outline">{bindings.length} 个动作步骤</Badge>
          </header>
          {bindings.map((binding, index) => (
            <article key={binding.stepId}>
              <div className="binding-step">
                <span>{index + 1}</span>
                <div>
                  <strong>{binding.stepName}</strong>
                  <code>{binding.action}</code>
                </div>
              </div>
              <label>
                <span>Plan UUID</span>
                <input
                  disabled={!canCreate}
                  onChange={(event) =>
                    updateBinding(binding.stepId, "planId", event.target.value)
                  }
                  placeholder="00000000-0000-4000-8000-000000000000"
                  required
                  value={binding.planId}
                />
              </label>
              <label>
                <span>Plan hash</span>
                <input
                  disabled={!canCreate}
                  onChange={(event) =>
                    updateBinding(binding.stepId, "planHash", event.target.value)
                  }
                  placeholder="sha256:…"
                  required
                  value={binding.planHash}
                />
              </label>
              <label>
                <span>Precondition hash</span>
                <input
                  disabled={!canCreate}
                  onChange={(event) =>
                    updateBinding(
                      binding.stepId,
                      "preconditionHash",
                      event.target.value,
                    )
                  }
                  placeholder="sha256:…"
                  required
                  value={binding.preconditionHash}
                />
              </label>
            </article>
          ))}
        </section>

        {message && <p className="form-message span-2">{message}</p>}
        <div className="form-actions span-2 schedule-form-actions">
          <Button
            disabled={!canCreate || !selected || busy !== undefined}
            type="submit"
          >
            <FileCheck2 size={15} />
            {busy === "preview" ? "正在预演…" : "预演排程"}
          </Button>
          <Button
            disabled={!preview?.schedulable || busy !== undefined}
            onClick={() => void createSchedule()}
            type="button"
            variant="outline"
          >
            <ShieldCheck size={15} />
            {busy === "create" ? "正在创建…" : "确认创建"}
          </Button>
          <Button
            disabled={!preview}
            onClick={() => setPreview(undefined)}
            type="button"
            variant="ghost"
          >
            <RotateCcw size={15} />
            清除预演
          </Button>
        </div>
      </form>
      {preview && (
        <ConflictPanel
          conflicts={preview.conflicts}
          schedulable={preview.schedulable}
        />
      )}
    </aside>
  );
}

function runbookKey(runbook: RunbookDefinition): string {
  return `${runbook.id}@${runbook.version}`;
}

function scheduleDefaults() {
  const startsAt = new Date(Date.now() + 60 * 60 * 1000);
  const endsAt = new Date(startsAt.getTime() + 30 * 60 * 1000);
  return {
    startsAt: toDateTimeLocal(startsAt),
    endsAt: toDateTimeLocal(endsAt),
  };
}
