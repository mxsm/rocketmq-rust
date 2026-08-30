import {
  CirclePause,
  LockKeyhole,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Snowflake,
} from "lucide-react";
import { useCallback, useState } from "react";

import { ApiError, type SreApi } from "@/api/client";
import type {
  AutonomyMode,
  AutonomyScopeView,
} from "@/api/types";
import { DataState } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useAsyncResource } from "@/hooks/useAsyncResource";

const MODE_LABELS: Record<AutonomyMode, string> = {
  disabled: "Disabled",
  shadow: "Shadow",
  supervised: "Supervised",
  autonomous: "Autonomous",
  paused: "Paused",
};

const VALID_TARGETS: Record<AutonomyMode, AutonomyMode[]> = {
  disabled: ["shadow"],
  shadow: ["supervised", "paused", "disabled"],
  supervised: ["autonomous", "paused", "disabled"],
  autonomous: ["paused", "disabled"],
  paused: ["shadow", "supervised", "disabled"],
};

type Editor =
  | { kind: "transition"; scope: AutonomyScopeView }
  | { kind: "freeze"; scope: AutonomyScopeView }
  | { kind: "kill-switch"; scope: AutonomyScopeView };

export function AutonomySettingsPanel({
  api,
  clusterId,
}: {
  api: SreApi;
  clusterId?: string;
}) {
  if (!clusterId) {
    return (
      <section className="autonomy-settings-select-cluster">
        <SlidersHorizontal aria-hidden="true" size={24} />
        <div>
          <strong>请选择单个授权集群</strong>
          <span>
            生命周期、冻结与 Kill Switch 都绑定精确集群和动作，不能跨集群批量修改。
          </span>
        </div>
      </section>
    );
  }
  return (
    <AutonomySettingsForCluster
      key={clusterId}
      api={api}
      clusterId={clusterId}
    />
  );
}

function AutonomySettingsForCluster({
  api,
  clusterId,
}: {
  api: SreApi;
  clusterId: string;
}) {
  const load = useCallback(
    (signal: AbortSignal) =>
      api.listAutonomyScopes(clusterId, 100, signal),
    [api, clusterId],
  );
  const resource = useAsyncResource(load);
  const [editor, setEditor] = useState<Editor>();
  const [targetMode, setTargetMode] = useState<AutonomyMode>("shadow");
  const [reason, setReason] = useState("");
  const [approvalReference, setApprovalReference] = useState("");
  const [ownerConfirmed, setOwnerConfirmed] = useState(false);
  const [freezeUntil, setFreezeUntil] = useState("");
  const [saving, setSaving] = useState(false);
  const [mutationError, setMutationError] = useState<string>();
  const [success, setSuccess] = useState<string>();
  const page = resource.data;

  const openEditor = (next: Editor) => {
    setEditor(next);
    setReason("");
    setApprovalReference("");
    setOwnerConfirmed(false);
    setMutationError(undefined);
    setSuccess(undefined);
    if (next.kind === "transition") {
      setTargetMode(
        VALID_TARGETS[next.scope.lifecycle.mode][0] ?? "disabled",
      );
    } else if (next.kind === "freeze") {
      setFreezeUntil(defaultFreezeUntil());
    }
  };

  const submit = async () => {
    if (!editor || saving) {
      return;
    }
    setSaving(true);
    setMutationError(undefined);
    try {
      const scope = editor.scope;
      if (editor.kind === "transition") {
        await api.transitionAutonomyScope(
          {
            clusterId: scope.policy.cluster_id,
            action: scope.policy.action,
            actionVersion: scope.policy.action_version,
          },
          {
            target_mode: targetMode,
            reason: reason.trim(),
            owner_confirmed: ownerConfirmed,
            ...(targetMode === "autonomous"
              ? { owner_approval_ref: approvalReference.trim() }
              : {}),
          },
        );
        setSuccess("生命周期变更已由服务端校验并记录");
      } else if (editor.kind === "freeze") {
        const active = scope.active_freezes.length === 0;
        await api.setAutonomyFreeze({
          cluster_id: scope.policy.cluster_id,
          action: scope.policy.action,
          action_version: scope.policy.action_version,
          active,
          reason: reason.trim(),
          starts_at: new Date().toISOString(),
          ...(active && freezeUntil
            ? { expires_at: new Date(freezeUntil).toISOString() }
            : {}),
        });
        setSuccess(active ? "动作 Freeze 已生效" : "动作 Freeze 已解除");
      } else {
        const active = !scope.kill_switch?.active;
        await api.setAutonomyKillSwitch({
          cluster_id: scope.policy.cluster_id,
          action: scope.policy.action,
          action_version: scope.policy.action_version,
          active,
          reason: reason.trim(),
        });
        setSuccess(
          active ? "Kill Switch 已启用" : "Kill Switch 已解除",
        );
      }
      setEditor(undefined);
      resource.reload();
    } catch (error) {
      setMutationError(
        error instanceof ApiError
          ? error.code + "：" + error.message
          : "自治控制请求失败，服务端未确认任何状态变更。",
      );
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="autonomy-settings-layout">
      <header className="autonomy-settings-header">
        <div>
          <span className="autonomy-settings-icon">
            <SlidersHorizontal aria-hidden="true" size={17} />
          </span>
          <div>
            <strong>Autonomy Settings</strong>
            <p>
              服务端资格判定是唯一权威来源；这里只允许人工 Operator 修改一个动作的生命周期和安全控制。
            </p>
          </div>
        </div>
        <div>
          <Badge variant="outline">human authority only</Badge>
          <Badge variant="success">typed control plane API</Badge>
          <Button
            aria-label="刷新自治模式治理"
            onClick={resource.reload}
            size="sm"
            variant="outline"
          >
            <RefreshCw
              className={resource.loading ? "spin" : undefined}
              size={14}
            />
            刷新
          </Button>
        </div>
      </header>

      {success && (
        <div className="autonomy-settings-success" role="status">
          <ShieldCheck aria-hidden="true" size={15} />
          {success}
        </div>
      )}
      <DataState
        empty={!resource.loading && !resource.error && page?.items.length === 0}
        emptyDescription="当前集群尚未配置任何动作自治 Policy。"
        emptyTitle="暂无自治作用域"
        error={resource.error}
        loading={resource.loading && !page}
        onRetry={resource.reload}
      />
      {page?.truncated && (
        <div className="autonomy-settings-warning">
          当前列表已达到 100 条上限，请使用更精确的服务端作用域查询。
        </div>
      )}

      <div className="autonomy-scope-grid">
        {page?.items.map((scope) => (
          <AutonomyScopeCard
            key={scope.policy.action + ":" + scope.policy.action_version}
            onEdit={openEditor}
            scope={scope}
          />
        ))}
      </div>

      {editor && (
        <AutonomyEditor
          approvalReference={approvalReference}
          editor={editor}
          freezeUntil={freezeUntil}
          mutationError={mutationError}
          onApprovalReferenceChange={setApprovalReference}
          onCancel={() => !saving && setEditor(undefined)}
          onFreezeUntilChange={setFreezeUntil}
          onOwnerConfirmedChange={setOwnerConfirmed}
          onReasonChange={setReason}
          onSubmit={() => void submit()}
          onTargetModeChange={setTargetMode}
          ownerConfirmed={ownerConfirmed}
          reason={reason}
          saving={saving}
          targetMode={targetMode}
        />
      )}
    </div>
  );
}

function AutonomyScopeCard({
  scope,
  onEdit,
}: {
  scope: AutonomyScopeView;
  onEdit: (editor: Editor) => void;
}) {
  const qualified = autonomousQualificationSatisfied(scope);
  const frozen = scope.active_freezes.length > 0;
  const killed = scope.kill_switch?.active === true;
  return (
    <article className="autonomy-scope-card">
      <header>
        <div>
          <strong>{scope.policy.action}</strong>
          <span>
            {scope.policy.action_version} · Owner {scope.policy.owner}
          </span>
        </div>
        <Badge variant={modeBadge(scope.lifecycle.mode)}>
          {MODE_LABELS[scope.lifecycle.mode]}
        </Badge>
      </header>
      <div className="autonomy-scope-status">
        <strong>当前模式：{MODE_LABELS[scope.lifecycle.mode]}</strong>
        <span>
          revision {scope.lifecycle.lifecycle_revision} ·{" "}
          {scope.lifecycle.updated_by} ·{" "}
          {formatCompactTime(scope.lifecycle.updated_at)}
        </span>
      </div>
      <div className="autonomy-scope-safety">
        <span className={qualified ? "safe" : "pending"}>
          <ShieldCheck aria-hidden="true" size={13} />
          {qualified ? "资格已满足" : "资格未满足"}
        </span>
        <span className={frozen ? "blocked" : "safe"}>
          <Snowflake aria-hidden="true" size={13} />
          {frozen ? "Freeze active" : "No active freeze"}
        </span>
        <span className={killed ? "blocked" : "safe"}>
          <ShieldAlert aria-hidden="true" size={13} />
          {killed ? "Kill Switch active" : "Kill Switch clear"}
        </span>
        <span className="live-check">
          <LockKeyhole aria-hidden="true" size={13} />
          Error budget 执行前实时复核
        </span>
      </div>
      <div className="autonomy-qualification-grid">
        <QualificationMetric
          label="Shadow 样本"
          value={
            scope.qualification.qualified_shadow_samples +
            "/" +
            scope.policy.min_shadow_samples
          }
        />
        <QualificationMetric
          label="Supervised 成功"
          value={
            scope.qualification.qualified_supervised_successes +
            "/" +
            scope.policy.min_supervised_successes
          }
        />
        <QualificationMetric
          label="Unknown / Rollback"
          value={
            scope.qualification.unresolved_unknown +
            " / " +
            scope.qualification.recent_rollbacks
          }
        />
        <QualificationMetric
          label="观察窗口"
          value={
            scope.qualification.autonomous_observation_window_met
              ? "met"
              : "pending"
          }
        />
      </div>
      <footer>
        <Button
          aria-label={"变更 " + scope.policy.action + " 模式"}
          onClick={() => onEdit({ kind: "transition", scope })}
          size="sm"
        >
          <SlidersHorizontal size={14} />
          变更模式
        </Button>
        <Button
          onClick={() => onEdit({ kind: "freeze", scope })}
          size="sm"
          variant="outline"
        >
          <Snowflake size={14} />
          {frozen ? "解除 Freeze" : "设置 Freeze"}
        </Button>
        <Button
          onClick={() => onEdit({ kind: "kill-switch", scope })}
          size="sm"
          variant={killed ? "default" : "destructive"}
        >
          <ShieldAlert size={14} />
          {killed ? "解除 Kill Switch" : "启用 Kill Switch"}
        </Button>
      </footer>
    </article>
  );
}

function QualificationMetric({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function AutonomyEditor(props: {
  approvalReference: string;
  editor: Editor;
  freezeUntil: string;
  mutationError?: string;
  onApprovalReferenceChange: (value: string) => void;
  onCancel: () => void;
  onFreezeUntilChange: (value: string) => void;
  onOwnerConfirmedChange: (value: boolean) => void;
  onReasonChange: (value: string) => void;
  onSubmit: () => void;
  onTargetModeChange: (value: AutonomyMode) => void;
  ownerConfirmed: boolean;
  reason: string;
  saving: boolean;
  targetMode: AutonomyMode;
}) {
  const {
    approvalReference,
    editor,
    freezeUntil,
    mutationError,
    onApprovalReferenceChange,
    onCancel,
    onFreezeUntilChange,
    onOwnerConfirmedChange,
    onReasonChange,
    onSubmit,
    onTargetModeChange,
    ownerConfirmed,
    reason,
    saving,
    targetMode,
  } = props;
  const autonomous =
    editor.kind === "transition" && targetMode === "autonomous";
  const ownerRequired =
    editor.kind === "transition" &&
    (targetMode === "supervised" || autonomous);
  const active =
    editor.kind === "freeze"
      ? editor.scope.active_freezes.length === 0
      : editor.kind === "kill-switch"
        ? !editor.scope.kill_switch?.active
        : undefined;
  const disabled =
    saving ||
    !reason.trim() ||
    reason.length > 512 ||
    (ownerRequired && !ownerConfirmed) ||
    (autonomous && !isBoundedApprovalReference(approvalReference));

  return (
    <aside aria-label={editorTitle(editor)} className="autonomy-editor">
      <header>
        <div>
          {editor.kind === "transition" ? (
            <SlidersHorizontal aria-hidden="true" size={18} />
          ) : editor.kind === "freeze" ? (
            <CirclePause aria-hidden="true" size={18} />
          ) : (
            <ShieldAlert aria-hidden="true" size={18} />
          )}
          <div>
            <strong>{editorTitle(editor)}</strong>
            <span>{editor.scope.policy.action}</span>
          </div>
        </div>
        <Badge variant="outline">server authoritative</Badge>
      </header>

      {editor.kind === "transition" && (
        <label>
          <span>目标模式</span>
          <Select
            value={targetMode}
            onValueChange={(value) =>
              onTargetModeChange(value as AutonomyMode)
            }
          >
            <SelectTrigger aria-label="目标模式">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {VALID_TARGETS[editor.scope.lifecycle.mode].map((mode) => (
                <SelectItem key={mode} value={mode}>
                  {MODE_LABELS[mode]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
      )}

      {editor.kind === "freeze" && active && (
        <label>
          <span>冻结到期时间</span>
          <Input
            aria-label="冻结到期时间"
            onChange={(event) => onFreezeUntilChange(event.target.value)}
            type="datetime-local"
            value={freezeUntil}
          />
        </label>
      )}

      <label className="autonomy-editor-reason">
        <span>变更原因</span>
        <textarea
          aria-label="变更原因"
          maxLength={512}
          onChange={(event) => onReasonChange(event.target.value)}
          placeholder="记录可审计的人工操作原因，不得包含凭据或消息正文"
          rows={3}
          value={reason}
        />
        <small>{reason.length}/512</small>
      </label>

      {autonomous && (
        <label>
          <span>审批引用</span>
          <Input
            aria-label="审批引用"
            maxLength={160}
            onChange={(event) =>
              onApprovalReferenceChange(event.target.value)
            }
            placeholder="approval://change/cab-2042"
            value={approvalReference}
          />
          <small>仅接受有界、不含敏感信息的 approval:// 引用。</small>
        </label>
      )}

      {ownerRequired && (
        <label className="autonomy-owner-confirmation">
          <input
            aria-label="生产 Owner 已确认"
            checked={ownerConfirmed}
            onChange={(event) =>
              onOwnerConfirmedChange(event.target.checked)
            }
            type="checkbox"
          />
          <span>
            生产 Owner 已确认；服务端仍会拒绝资格不足、Freeze、Kill Switch
            或错误预算不足的请求。
          </span>
        </label>
      )}

      {mutationError && (
        <div className="autonomy-editor-error" role="alert">
          <ShieldAlert aria-hidden="true" size={15} />
          {mutationError}
        </div>
      )}

      <footer>
        <Button disabled={saving} onClick={onCancel} variant="outline">
          取消
        </Button>
        <Button disabled={disabled} onClick={onSubmit}>
          {saving
            ? "服务端校验中…"
            : editor.kind === "transition"
              ? "确认模式变更"
              : active
                ? "确认启用"
                : "确认解除"}
        </Button>
      </footer>
    </aside>
  );
}

function autonomousQualificationSatisfied(scope: AutonomyScopeView) {
  const qualification = scope.qualification;
  return (
    qualification.autonomous_cohort !== null &&
    qualification.autonomous_observation_window_met &&
    qualification.qualified_supervised_successes >=
      scope.policy.min_supervised_successes &&
    qualification.unresolved_unknown <=
      scope.policy.max_unresolved_unknown &&
    qualification.recent_rollbacks <= scope.policy.max_recent_rollbacks
  );
}

function isBoundedApprovalReference(reference: string) {
  const suffix = reference.startsWith("approval://")
    ? reference.slice("approval://".length)
    : "";
  return (
    reference.trim() === reference &&
    reference.length <= 160 &&
    suffix.length >= 3 &&
    !suffix.includes("..") &&
    !suffix.includes("//") &&
    /^[a-z0-9](?:[a-z0-9._/-]*[a-z0-9])$/u.test(suffix)
  );
}

function editorTitle(editor: Editor) {
  if (editor.kind === "transition") {
    return "变更自治模式";
  }
  if (editor.kind === "freeze") {
    return editor.scope.active_freezes.length > 0
      ? "解除动作 Freeze"
      : "设置动作 Freeze";
  }
  return editor.scope.kill_switch?.active
    ? "解除 Kill Switch"
    : "启用 Kill Switch";
}

function modeBadge(mode: AutonomyMode) {
  if (mode === "autonomous" || mode === "supervised") {
    return "success" as const;
  }
  if (mode === "paused") {
    return "warning" as const;
  }
  return "outline" as const;
}

function formatCompactTime(value: string) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime())
    ? "unknown"
    : new Intl.DateTimeFormat("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).format(parsed);
}

function defaultFreezeUntil() {
  const date = new Date(Date.now() + 2 * 60 * 60 * 1000);
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}
