import {
  AlertTriangle,
  CheckCircle2,
  CirclePause,
  ClipboardCheck,
  Play,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
} from "lucide-react";
import { type ReactNode, useMemo, useState } from "react";

import type {
  CompleteRollbackRequest,
  PrepareReleaseRequest,
  RecordReleaseObservationRequest,
  ReleaseExecutionRequest,
  ReleaseObservationPhase,
  ReleaseTransitionRequest,
  ReleaseWorkflow,
} from "@/api/types";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export interface ReleaseDetailActions {
  prepare: (input: PrepareReleaseRequest) => Promise<void>;
  start: (input: ReleaseExecutionRequest) => Promise<void>;
  observe: (input: RecordReleaseObservationRequest) => Promise<void>;
  pause: (input: ReleaseTransitionRequest) => Promise<void>;
  resume: (input: ReleaseTransitionRequest) => Promise<void>;
  beginVerification: () => Promise<void>;
  complete: () => Promise<void>;
  startRollback: (input: ReleaseExecutionRequest) => Promise<void>;
  completeRollback: (input: CompleteRollbackRequest) => Promise<void>;
  manualTakeover: (input: ReleaseTransitionRequest) => Promise<void>;
}

export function ReleaseDetailControls({
  workflow,
  actions,
  busy,
}: {
  workflow: ReleaseWorkflow;
  actions: ReleaseDetailActions;
  busy?: string;
}) {
  const planned = ["planned", "readiness_checking"].includes(
    workflow.status,
  );
  const observing = [
    "ready",
    "canary_running",
    "paused",
    "verifying",
  ].includes(workflow.status);
  const terminal = [
    "completed",
    "rolled_back",
    "manual_takeover",
    "failed",
  ].includes(workflow.status);

  return (
    <aside className="release-control-stack" aria-label="发布护航控制">
      <header className="release-control-heading">
        <div>
          <span className="section-kicker">SUPERVISED CONTROL</span>
          <h2>护航控制台</h2>
          <p>每个操作都会重新验证不可变 Plan、审批、租约和 fencing token。</p>
        </div>
        {busy && (
          <span className="release-busy">
            <RefreshCw aria-hidden="true" className="spin" size={13} />
            {busy}
          </span>
        )}
      </header>

      {planned && (
        <PreparationControl
          disabled={Boolean(busy)}
          onSubmit={actions.prepare}
        />
      )}
      {workflow.status === "ready" && (
        <ExecutionControl
          actionLabel="开始 Canary"
          description="使用已批准 Plan 的前置条件 hash 创建受监管执行。"
          disabled={Boolean(busy)}
          icon={<Play aria-hidden="true" size={15} />}
          onSubmit={actions.start}
          planHash={workflow.plan_hash}
        />
      )}
      {observing && (
        <ObservationControl
          disabled={Boolean(busy)}
          onSubmit={actions.observe}
          suggestedPhase={
            workflow.status === "ready"
              ? "before"
              : workflow.status === "verifying"
                ? "after"
                : "during"
          }
        />
      )}
      {workflow.status === "canary_running" && (
        <TransitionControl
          actionLabel="暂停并保留执行上下文"
          description="适用于值班判断或非自动触发的风险信号。"
          disabled={Boolean(busy)}
          icon={<CirclePause aria-hidden="true" size={15} />}
          onSubmit={actions.pause}
          tone="warning"
        />
      )}
      {workflow.status === "paused" && (
        <TransitionControl
          actionLabel="确认风险消除并恢复"
          description="恢复前仍会重新检查同一 Plan 与执行租约。"
          disabled={Boolean(busy)}
          icon={<Play aria-hidden="true" size={15} />}
          onSubmit={actions.resume}
        />
      )}
      {["canary_running", "paused"].includes(workflow.status) && (
        <Button
          className="release-control-primary"
          disabled={Boolean(busy)}
          onClick={() => void actions.beginVerification()}
          type="button"
          variant="outline"
        >
          <ClipboardCheck aria-hidden="true" size={15} />
          进入发布后验证
        </Button>
      )}
      {workflow.status === "verifying" && (
        <Button
          className="release-control-primary"
          disabled={Boolean(busy)}
          onClick={() => void actions.complete()}
          type="button"
        >
          <CheckCircle2 aria-hidden="true" size={15} />
          完成发布并生成报告
        </Button>
      )}
      {["canary_running", "paused", "verifying"].includes(
        workflow.status,
      ) &&
        workflow.rollback_plan_id && (
          <ExecutionControl
            actionLabel="启动类型化回滚"
            description="只执行已审批的 Rollback Plan，不接受临时命令。"
            disabled={Boolean(busy)}
            icon={<RotateCcw aria-hidden="true" size={15} />}
            onSubmit={actions.startRollback}
            planHash={workflow.rollback_plan_hash ?? ""}
            tone="danger"
          />
        )}
      {workflow.status === "rolling_back" && (
        <RollbackCompletionControl
          disabled={Boolean(busy)}
          onSubmit={actions.completeRollback}
        />
      )}
      {!terminal && (
        <TransitionControl
          actionLabel="进入人工接管"
          description="停止自动推进并保留完整审计证据，不直接操作 RocketMQ。"
          disabled={Boolean(busy)}
          icon={<ShieldAlert aria-hidden="true" size={15} />}
          onSubmit={actions.manualTakeover}
          tone="danger"
        />
      )}
      {terminal && (
        <div className="release-control-terminal">
          <CheckCircle2 aria-hidden="true" size={18} />
          <div>
            <strong>自动护航已结束</strong>
            <span>状态与报告不可变；如需新动作，请创建新的审批 Plan。</span>
          </div>
        </div>
      )}
    </aside>
  );
}

function PreparationControl({
  disabled,
  onSubmit,
}: {
  disabled: boolean;
  onSubmit: (input: PrepareReleaseRequest) => Promise<void>;
}) {
  const [pdbReady, setPdbReady] = useState(true);
  const [probeReady, setProbeReady] = useState(true);
  const [evidenceIds, setEvidenceIds] = useState("");
  const [resourceKeys, setResourceKeys] = useState("");
  const [configurationChanges, setConfigurationChanges] = useState("");

  return (
    <form
      className="release-control-card"
      onSubmit={(event) => {
        event.preventDefault();
        void onSubmit({
          pdb_ready: pdbReady,
          synthetic_probe_ready: probeReady,
          evidence_ids: splitValues(evidenceIds),
          affected_resource_keys: splitValues(resourceKeys),
          configuration_changes: splitValues(configurationChanges),
        });
      }}
    >
      <ControlTitle
        icon={<ClipboardCheck aria-hidden="true" size={16} />}
        title="运行确定性准备检查"
      />
      <div className="release-toggle-row">
        <CheckField
          checked={pdbReady}
          label="PodDisruptionBudget 已验证"
          onChange={setPdbReady}
        />
        <CheckField
          checked={probeReady}
          label="Synthetic Probe 已通过"
          onChange={setProbeReady}
        />
      </div>
      <ControlField
        label="Evidence UUID（逗号分隔）"
        onChange={setEvidenceIds}
        placeholder="evidence UUID"
        required
        value={evidenceIds}
      />
      <ControlField
        label="受影响资源键"
        onChange={setResourceKeys}
        placeholder="broker:broker-a, proxy:proxy-0"
        value={resourceKeys}
      />
      <ControlField
        label="配置变更摘要"
        onChange={setConfigurationChanges}
        placeholder="broker image 5.3.0"
        value={configurationChanges}
      />
      <Button disabled={disabled} type="submit">
        <ClipboardCheck aria-hidden="true" size={15} />
        评估 readiness 与 what-if
      </Button>
    </form>
  );
}

function ExecutionControl({
  actionLabel,
  description,
  disabled,
  icon,
  onSubmit,
  planHash,
  tone,
}: {
  actionLabel: string;
  description: string;
  disabled: boolean;
  icon: ReactNode;
  onSubmit: (input: ReleaseExecutionRequest) => Promise<void>;
  planHash: string;
  tone?: "danger";
}) {
  const [preconditionHash, setPreconditionHash] = useState(planHash);
  const idempotencyKey = useMemo(() => crypto.randomUUID(), []);

  return (
    <form
      className={`release-control-card${tone ? ` ${tone}` : ""}`}
      onSubmit={(event) => {
        event.preventDefault();
        void onSubmit({
          precondition_hash: preconditionHash.trim(),
          idempotency_key: idempotencyKey,
        });
      }}
    >
      <ControlTitle icon={icon} title={actionLabel} />
      <p>{description}</p>
      <ControlField
        label="前置条件 hash"
        maxLength={71}
        onChange={setPreconditionHash}
        placeholder="sha256:…"
        required
        value={preconditionHash}
      />
      <code className="release-idempotency">
        idempotency · {idempotencyKey}
      </code>
      <Button
        disabled={disabled}
        type="submit"
        variant={tone === "danger" ? "destructive" : "default"}
      >
        {icon}
        {actionLabel}
      </Button>
    </form>
  );
}

function ObservationControl({
  disabled,
  onSubmit,
  suggestedPhase,
}: {
  disabled: boolean;
  onSubmit: (input: RecordReleaseObservationRequest) => Promise<void>;
  suggestedPhase: ReleaseObservationPhase;
}) {
  const [phase, setPhase] =
    useState<ReleaseObservationPhase>(suggestedPhase);
  const [sloHealthy, setSloHealthy] = useState(true);
  const [probeHealthy, setProbeHealthy] = useState(true);
  const [evidenceIds, setEvidenceIds] = useState("");
  const [summary, setSummary] = useState("");

  return (
    <form
      className="release-control-card"
      onSubmit={(event) => {
        event.preventDefault();
        void onSubmit({
          phase,
          slo_healthy: sloHealthy,
          synthetic_probe_healthy: probeHealthy,
          evidence_ids: splitValues(evidenceIds),
          sanitized_summary: summary.trim(),
        });
      }}
    >
      <ControlTitle
        icon={<AlertTriangle aria-hidden="true" size={16} />}
        title="记录有界护航观察"
      />
      <Select
        value={phase}
        onValueChange={(value) =>
          setPhase(value as ReleaseObservationPhase)
        }
      >
        <SelectTrigger aria-label="观察阶段">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="before">发布前</SelectItem>
          <SelectItem value="during">发布中</SelectItem>
          <SelectItem value="after">发布后</SelectItem>
        </SelectContent>
      </Select>
      <div className="release-toggle-row">
        <CheckField
          checked={sloHealthy}
          label="SLO 正常"
          onChange={setSloHealthy}
        />
        <CheckField
          checked={probeHealthy}
          label="Synthetic Probe 正常"
          onChange={setProbeHealthy}
        />
      </div>
      <ControlField
        label="Evidence UUID（逗号分隔）"
        onChange={setEvidenceIds}
        placeholder="evidence UUID"
        required
        value={evidenceIds}
      />
      <ControlField
        label="脱敏观察摘要"
        maxLength={1024}
        onChange={setSummary}
        placeholder="5 分钟窗口内错误率与 P99 保持基线"
        required
        value={summary}
      />
      <Button disabled={disabled} type="submit" variant="outline">
        记录观察
      </Button>
    </form>
  );
}

function TransitionControl({
  actionLabel,
  description,
  disabled,
  icon,
  onSubmit,
  tone,
}: {
  actionLabel: string;
  description: string;
  disabled: boolean;
  icon: ReactNode;
  onSubmit: (input: ReleaseTransitionRequest) => Promise<void>;
  tone?: "warning" | "danger";
}) {
  const [reason, setReason] = useState("");

  return (
    <form
      className={`release-control-card${tone ? ` ${tone}` : ""}`}
      onSubmit={(event) => {
        event.preventDefault();
        void onSubmit({ reason: reason.trim() });
      }}
    >
      <ControlTitle icon={icon} title={actionLabel} />
      <p>{description}</p>
      <ControlField
        label="审计原因"
        maxLength={1024}
        onChange={setReason}
        placeholder="填写不包含敏感信息的操作原因"
        required
        value={reason}
      />
      <Button
        disabled={disabled}
        type="submit"
        variant={tone === "danger" ? "destructive" : "outline"}
      >
        {icon}
        {actionLabel}
      </Button>
    </form>
  );
}

function RollbackCompletionControl({
  disabled,
  onSubmit,
}: {
  disabled: boolean;
  onSubmit: (input: CompleteRollbackRequest) => Promise<void>;
}) {
  const [succeeded, setSucceeded] = useState(true);
  const [sloHealthy, setSloHealthy] = useState(true);
  const [probeHealthy, setProbeHealthy] = useState(true);
  const [reason, setReason] = useState("");
  const [summary, setSummary] = useState("");
  const [evidenceIds, setEvidenceIds] = useState("");

  return (
    <form
      className="release-control-card warning"
      onSubmit={(event) => {
        event.preventDefault();
        void onSubmit({
          succeeded,
          reason: reason.trim(),
          observation: {
            phase: "after",
            slo_healthy: sloHealthy,
            synthetic_probe_healthy: probeHealthy,
            evidence_ids: splitValues(evidenceIds),
            sanitized_summary: summary.trim(),
          },
        });
      }}
    >
      <ControlTitle
        icon={<RotateCcw aria-hidden="true" size={16} />}
        title="核对回滚结果"
      />
      <div className="release-toggle-row">
        <CheckField
          checked={succeeded}
          label="回滚动作成功"
          onChange={setSucceeded}
        />
        <CheckField
          checked={sloHealthy && probeHealthy}
          label="SLO 与 Probe 已恢复"
          onChange={(checked) => {
            setSloHealthy(checked);
            setProbeHealthy(checked);
          }}
        />
      </div>
      <ControlField
        label="回滚原因"
        maxLength={1024}
        onChange={setReason}
        placeholder="触发回滚的脱敏原因"
        required
        value={reason}
      />
      <ControlField
        label="恢复观察摘要"
        maxLength={1024}
        onChange={setSummary}
        placeholder="回滚后稳定窗口与关键 SLO"
        required
        value={summary}
      />
      <ControlField
        label="Evidence UUID（逗号分隔）"
        onChange={setEvidenceIds}
        placeholder="evidence UUID"
        required
        value={evidenceIds}
      />
      <Button disabled={disabled} type="submit" variant="outline">
        完成回滚核对并生成报告
      </Button>
    </form>
  );
}

function ControlTitle({
  icon,
  title,
}: {
  icon: ReactNode;
  title: string;
}) {
  return (
    <header className="release-control-title">
      <span>{icon}</span>
      <strong>{title}</strong>
    </header>
  );
}

function ControlField({
  label,
  value,
  placeholder,
  onChange,
  maxLength = 1024,
  required = false,
}: {
  label: string;
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  maxLength?: number;
  required?: boolean;
}) {
  return (
    <label className="release-control-field">
      <span>{label}</span>
      <input
        maxLength={maxLength}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        required={required}
        value={value}
      />
    </label>
  );
}

function CheckField({
  checked,
  label,
  onChange,
}: {
  checked: boolean;
  label: string;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="release-check-field">
      <input
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        type="checkbox"
      />
      <span>{label}</span>
    </label>
  );
}

function splitValues(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}
