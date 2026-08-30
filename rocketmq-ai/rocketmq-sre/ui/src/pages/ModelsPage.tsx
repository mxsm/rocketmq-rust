import {
  Activity,
  Bot,
  CheckCircle2,
  FlaskConical,
  Gauge,
  LoaderCircle,
  RotateCcw,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useMemo,
  useState,
} from "react";

import { ApiError } from "@/api/client";
import type {
  ModelProfileLifecycleState,
  ModelProfileLifecycleView,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import {
  DataState,
  DataSurface,
  DefinitionGrid,
  formatTime,
} from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { useSreData } from "@/data/SreDataContext";
import {
  allowedLifecycleTransitions,
  canGovernModels,
  hasPassingSmoke,
  lifecycleBadgeVariant,
  lifecycleLabels,
  summarizeModelGovernance,
  transitionLabels,
} from "@/features/models/modelLifecycle";
import { useAsyncResource } from "@/hooks/useAsyncResource";

const SAFE_REASON_CODE = /^[A-Za-z0-9_.:-]{1,128}$/;

export function ModelsPage() {
  const { api } = useSreData();
  const auth = useAuth();
  const load = useCallback(
    async (signal: AbortSignal) => {
      const [capabilities, lifecycle] = await Promise.all([
        api.getModelCapabilities(signal),
        api.listModelProfileLifecycles(signal),
      ]);
      return { capabilities, lifecycle };
    },
    [api],
  );
  const resource = useAsyncResource(load);
  const [selectedId, setSelectedId] = useState("");
  const [targetState, setTargetState] =
    useState<ModelProfileLifecycleState>("certified");
  const [rollbackProfileId, setRollbackProfileId] = useState("none");
  const [reasonCode, setReasonCode] = useState(
    "operator.lifecycle_change",
  );
  const [operatorConfirmed, setOperatorConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [operation, setOperation] = useState<{
    tone: "success" | "warning";
    message: string;
  }>();

  const profiles = useMemo(
    () => resource.data?.lifecycle.items ?? [],
    [resource.data?.lifecycle.items],
  );
  const selected =
    profiles.find((profile) => profile.profile_id === selectedId) ??
    profiles[0];
  const summary = useMemo(
    () => summarizeModelGovernance(profiles),
    [profiles],
  );
  const canGovern = canGovernModels(auth.session?.roles ?? []);
  const transitions = selected
    ? allowedLifecycleTransitions(selected.state)
    : [];
  const effectiveTarget = transitions.includes(targetState)
    ? targetState
    : transitions[0];
  const rollbackCandidates = profiles.filter(
    (profile) =>
      profile.profile_id !== selected?.profile_id &&
      ["certified", "promoted"].includes(profile.state) &&
      hasPassingSmoke(profile),
  );
  const effectiveRollbackProfileId =
    rollbackProfileId !== "none" &&
    rollbackCandidates.some(
      (profile) => profile.profile_id === rollbackProfileId,
    )
      ? rollbackProfileId
      : (rollbackCandidates[0]?.profile_id ?? "none");
  const selectedCapabilities =
    resource.data?.capabilities.profiles?.find(
      (profile) => profile.id === selected?.profile_id,
    );
  const reasonValid = SAFE_REASON_CODE.test(reasonCode);
  const smokeRequired =
    effectiveTarget !== undefined &&
    ["certified", "promoted"].includes(effectiveTarget) &&
    selected !== undefined &&
    !hasPassingSmoke(selected);
  const actionDisabled =
    !canGovern ||
    busy ||
    !selected ||
    !effectiveTarget ||
    !operatorConfirmed ||
    !reasonValid ||
    smokeRequired;

  const selectProfile = (profile: ModelProfileLifecycleView) => {
    setSelectedId(profile.profile_id);
    setTargetState(
      allowedLifecycleTransitions(profile.state)[0] ?? "retired",
    );
    setRollbackProfileId("none");
    setOperatorConfirmed(false);
    setOperation(undefined);
  };

  const applyTransition = async (event: FormEvent) => {
    event.preventDefault();
    if (!selected || !effectiveTarget || actionDisabled) {
      return;
    }
    setBusy(true);
    setOperation(undefined);
    try {
      const updated = await api.transitionModelProfileLifecycle(
        selected.profile_id,
        {
          target_state: effectiveTarget,
          expected_revision: selected.revision,
          rollback_profile_id:
            effectiveTarget === "promoted" &&
            effectiveRollbackProfileId !== "none"
              ? effectiveRollbackProfileId
              : undefined,
          reason_code: reasonCode,
          operator_confirmed: true,
        },
      );
      setSelectedId(updated.profile_id);
      setOperatorConfirmed(false);
      setOperation({
        tone: "success",
        message: `${updated.profile_name} 已进入${lifecycleLabels[updated.state]}，revision ${updated.revision}。`,
      });
      resource.reload();
    } catch (error) {
      setOperation({ tone: "warning", message: modelOperationError(error) });
    } finally {
      setBusy(false);
    }
  };

  const applyRollback = async () => {
    if (
      !selected ||
      selected.state !== "promoted" ||
      !selected.rollback_profile_id ||
      !canGovern ||
      !operatorConfirmed ||
      !reasonValid
    ) {
      return;
    }
    setBusy(true);
    setOperation(undefined);
    try {
      const restored = await api.rollbackModelProfile(
        selected.profile_id,
        {
          expected_revision: selected.revision,
          reason_code: reasonCode,
          operator_confirmed: true,
        },
      );
      setSelectedId(restored.profile_id);
      setOperatorConfirmed(false);
      setOperation({
        tone: "success",
        message: `已回滚到 ${restored.profile_name}，原 Profile 自动隔离。`,
      });
      resource.reload();
    } catch (error) {
      setOperation({ tone: "warning", message: modelOperationError(error) });
    } finally {
      setBusy(false);
    }
  };

  const runSmoke = async () => {
    if (!selected || !canGovern || busy) {
      return;
    }
    setBusy(true);
    setOperation(undefined);
    try {
      const result = await api.runModelProfileSmoke(selected.profile_id);
      setOperation({
        tone: result.overall_ok ? "success" : "warning",
        message: result.overall_ok
          ? `基础 smoke 通过，耗时 ${result.latency_ms ?? "unknown"} ms。`
          : `基础 smoke 失败：${result.failure_codes.join(", ") || "provider_smoke_failed"}。`,
      });
      resource.reload();
    } catch (error) {
      setOperation({ tone: "warning", message: modelOperationError(error) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page model-governance-page">
      <PageHeader
        eyebrow="MODEL GOVERNANCE"
        title="模型生命周期与 Provider Health"
        description="认证、提升、隔离、回滚和持续 smoke；不显示 credential、token 或完整 endpoint。"
      />
      <section className="model-governance-boundary">
        <ShieldCheck aria-hidden="true" size={18} />
        <div>
          <strong>AI 路由治理，不是 RocketMQ 集群变更</strong>
          <p>
            动作只影响 SRE 模型 Profile；需要 model-governance
            角色、当前 revision、reason code 与人工确认。
          </p>
        </div>
        <code>cluster_mutation=false</code>
      </section>
      <DataState
        loading={resource.loading && !resource.data}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <>
          <section className="model-governance-summary">
            <GovernanceMetric
              icon={<Gauge size={18} />}
              label="生产路由"
              value={summary.promoted}
              detail="Promoted profiles"
            />
            <GovernanceMetric
              icon={<Activity size={18} />}
              label="Smoke 通过"
              value={summary.healthy}
              detail={`${profiles.length} 个受管 Profile`}
              tone="success"
            />
            <GovernanceMetric
              icon={<ShieldAlert size={18} />}
              label="隔离"
              value={summary.quarantined}
              detail="自动闭环已暂停"
              tone={summary.quarantined > 0 ? "warning" : "success"}
            />
            <GovernanceMetric
              icon={<Bot size={18} />}
              label="自治可用"
              value={summary.automationEligible}
              detail="实际路由 + 健康认证"
            />
          </section>

          <Tabs className="model-governance-tabs" defaultValue="lifecycle">
            <TabsList>
              <TabsTrigger value="lifecycle">生命周期与健康</TabsTrigger>
              <TabsTrigger value="protocols">协议适配矩阵</TabsTrigger>
            </TabsList>
            <TabsContent value="lifecycle">
              <div className="model-governance-workspace">
                <DataSurface
                  className="model-lifecycle-list"
                  title="受管 Profile"
                  description="状态与 smoke 来自持久化治理记录；missing 不会按 healthy 展示。"
                  meta={
                    <span>
                      observed {formatTime(resource.data.lifecycle.observed_at)}
                    </span>
                  }
                >
                  <div className="table-scroll">
                    <table className="model-lifecycle-table">
                      <thead>
                        <tr>
                          <th>Profile</th>
                          <th>生命周期</th>
                          <th>Provider / Model</th>
                          <th>最新 smoke</th>
                          <th>自治</th>
                          <th>Revision</th>
                        </tr>
                      </thead>
                      <tbody>
                        {profiles.map((profile) => (
                          <tr
                            className={
                              profile.profile_id === selected?.profile_id
                                ? "selected"
                                : undefined
                            }
                            key={profile.profile_id}
                          >
                            <td>
                              <button
                                className="model-profile-select"
                                onClick={() => selectProfile(profile)}
                                type="button"
                              >
                                <strong>{profile.profile_name}</strong>
                                <span>{shortId(profile.profile_id)}</span>
                              </button>
                            </td>
                            <td>
                              <Badge
                                variant={lifecycleBadgeVariant(
                                  profile.state,
                                )}
                              >
                                {lifecycleLabels[profile.state]}
                              </Badge>
                            </td>
                            <td>
                              <strong>{profile.provider_family}</strong>
                              <span>
                                {profile.model_family} ·{" "}
                                {profile.model_revision}
                              </span>
                            </td>
                            <td>
                              <SmokeCell profile={profile} />
                            </td>
                            <td>
                              <Badge
                                variant={
                                  profile.automation_eligible
                                    ? "success"
                                    : "outline"
                                }
                              >
                                {profile.automation_eligible
                                  ? "eligible"
                                  : "paused"}
                              </Badge>
                            </td>
                            <td>
                              <code>r{profile.revision}</code>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </DataSurface>

                {selected && (
                  <aside className="model-governance-panel">
                    <header>
                      <div>
                        <span>SELECTED PROFILE</span>
                        <h2>{selected.profile_name}</h2>
                      </div>
                      <Badge
                        variant={lifecycleBadgeVariant(selected.state)}
                      >
                        {lifecycleLabels[selected.state]}
                      </Badge>
                    </header>
                    <DefinitionGrid
                      items={[
                        {
                          label: "Provider",
                          value: selected.provider_family,
                        },
                        {
                          label: "Model family",
                          value: selected.model_family,
                        },
                        {
                          label: "Revision",
                          value: `r${selected.revision}`,
                          mono: true,
                        },
                        {
                          label: "Region",
                          value: selectedCapabilities?.region ?? "未返回",
                        },
                        {
                          label: "Rollback target",
                          value: selected.rollback_profile_id
                            ? profileName(
                                profiles,
                                selected.rollback_profile_id,
                              )
                            : "未设置",
                        },
                        {
                          label: "Updated by",
                          value: selected.updated_by,
                        },
                      ]}
                    />
                    <ProviderChecks profile={selected} />
                    <div className="model-panel-toolbar">
                      <Button
                        disabled={!canGovern || busy}
                        onClick={runSmoke}
                        size="sm"
                        type="button"
                        variant="outline"
                      >
                        {busy ? (
                          <LoaderCircle className="spin" size={14} />
                        ) : (
                          <FlaskConical size={14} />
                        )}
                        运行基础 smoke
                      </Button>
                      <span>
                        {formatTime(selected.latest_smoke?.observed_at)}
                      </span>
                    </div>
                    {!canGovern && (
                      <div className="inline-alert warning">
                        <TriangleAlert size={15} />
                        当前身份仅可查看；治理动作需要 model-governance
                        角色。
                      </div>
                    )}
                    {operation && (
                      <div
                        className={`inline-alert ${operation.tone === "warning" ? "warning" : ""}`.trim()}
                        role="status"
                      >
                        {operation.tone === "warning" ? (
                          <TriangleAlert size={15} />
                        ) : (
                          <CheckCircle2 size={15} />
                        )}
                        {operation.message}
                      </div>
                    )}
                    {transitions.length > 0 ? (
                      <form
                        className="model-lifecycle-form"
                        onSubmit={applyTransition}
                      >
                        <label>
                          <span>目标状态</span>
                          <Select
                            onValueChange={(value) =>
                              setTargetState(
                                value as ModelProfileLifecycleState,
                              )
                            }
                            value={effectiveTarget}
                          >
                            <SelectTrigger aria-label="目标生命周期状态">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {transitions.map((state) => (
                                <SelectItem key={state} value={state}>
                                  {transitionLabels[state]}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </label>
                        {effectiveTarget === "promoted" && (
                          <label>
                            <span>回滚目标</span>
                            <Select
                              onValueChange={setRollbackProfileId}
                              value={effectiveRollbackProfileId}
                            >
                              <SelectTrigger aria-label="模型回滚目标">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {rollbackCandidates.map((profile) => (
                                  <SelectItem
                                    key={profile.profile_id}
                                    value={profile.profile_id}
                                  >
                                    {profile.profile_name} ·{" "}
                                    {lifecycleLabels[profile.state]}
                                  </SelectItem>
                                ))}
                                <SelectItem value="none">
                                  不设置（首次引导）
                                </SelectItem>
                              </SelectContent>
                            </Select>
                          </label>
                        )}
                        <label>
                          <span>Reason code</span>
                          <input
                            aria-invalid={!reasonValid}
                            maxLength={128}
                            onChange={(event) =>
                              setReasonCode(event.target.value)
                            }
                            spellCheck={false}
                            value={reasonCode}
                          />
                          {!reasonValid && (
                            <small>
                              仅允许字母、数字、点、下划线、冒号和连字符。
                            </small>
                          )}
                        </label>
                        <label className="model-confirmation">
                          <input
                            checked={operatorConfirmed}
                            onChange={(event) =>
                              setOperatorConfirmed(event.target.checked)
                            }
                            type="checkbox"
                          />
                          <span>
                            我已核对 smoke、revision、回滚目标和路由影响
                          </span>
                        </label>
                        {smokeRequired && (
                          <div className="inline-alert warning">
                            认证或提升前必须先通过基础 smoke。
                          </div>
                        )}
                        <div className="model-lifecycle-actions">
                          <Button disabled={actionDisabled} type="submit">
                            <ShieldCheck size={14} />
                            {effectiveTarget
                              ? transitionLabels[effectiveTarget]
                              : "无可用转换"}
                          </Button>
                          {selected.state === "promoted" &&
                            selected.rollback_profile_id && (
                              <Button
                                disabled={
                                  !canGovern ||
                                  busy ||
                                  !operatorConfirmed ||
                                  !reasonValid
                                }
                                onClick={applyRollback}
                                type="button"
                                variant="destructive"
                              >
                                <RotateCcw size={14} />
                                回滚路由
                              </Button>
                            )}
                        </div>
                      </form>
                    ) : (
                      <div className="model-terminal-state">
                        <ShieldAlert size={18} />
                        <div>
                          <strong>Retired 是终态</strong>
                          <span>该 Profile 不能重新进入路由或自动闭环。</span>
                        </div>
                      </div>
                    )}
                  </aside>
                )}
              </div>
            </TabsContent>
            <TabsContent value="protocols">
              <ProtocolMatrix
                providers={resource.data.capabilities.providers}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
}

function GovernanceMetric({
  icon,
  label,
  value,
  detail,
  tone,
}: {
  icon: ReactNode;
  label: string;
  value: number;
  detail: string;
  tone?: "success" | "warning";
}) {
  return (
    <article className={tone ? `tone-${tone}` : undefined}>
      <span className="model-summary-icon">{icon}</span>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
    </article>
  );
}

function SmokeCell({ profile }: { profile: ModelProfileLifecycleView }) {
  if (!profile.latest_smoke) {
    return (
      <span className="model-smoke-status unknown">
        <TriangleAlert size={13} />
        missing
      </span>
    );
  }
  return (
    <span
      className={`model-smoke-status ${profile.latest_smoke.overall_ok ? "passed" : "failed"}`}
    >
      {profile.latest_smoke.overall_ok ? (
        <CheckCircle2 size={13} />
      ) : (
        <TriangleAlert size={13} />
      )}
      {profile.latest_smoke.overall_ok ? "passed" : "failed"}
      <small>{profile.latest_smoke.latency_ms ?? "?"} ms</small>
    </span>
  );
}

function ProviderChecks({
  profile,
}: {
  profile: ModelProfileLifecycleView;
}) {
  const smoke = profile.latest_smoke;
  const checks = [
    ["Connectivity", smoke?.connectivity_ok],
    ["Structured JSON", smoke?.structured_output_ok],
    ["Tool arguments", smoke?.tool_arguments_ok],
    ["Evidence citation", smoke?.evidence_citation_ok],
  ] as const;
  return (
    <section className="provider-checks">
      <header>
        <span>PROVIDER HEALTH</span>
        <Badge
          variant={
            smoke?.overall_ok
              ? "success"
              : smoke
                ? "destructive"
                : "secondary"
          }
        >
          {smoke?.overall_ok ? "passing" : smoke ? "failed" : "missing"}
        </Badge>
      </header>
      <div>
        {checks.map(([label, passed]) => (
          <span className={passed ? "passed" : "failed"} key={label}>
            {passed ? (
              <CheckCircle2 size={13} />
            ) : (
              <TriangleAlert size={13} />
            )}
            {label}
          </span>
        ))}
      </div>
      {smoke && smoke.failure_codes.length > 0 && (
        <code>{smoke.failure_codes.join(" · ")}</code>
      )}
    </section>
  );
}

function ProtocolMatrix({
  providers,
}: {
  providers: Array<{
    id: string;
    protocols: string[];
    supports_streaming: boolean;
    supports_tools: boolean;
    supports_structured_output: boolean;
    supports_embeddings: boolean;
  }>;
}) {
  return (
    <DataSurface
      title="协议适配矩阵"
      description="包含 DeepSeek、智谱 GLM 与 Kimi/Moonshot；能力取自 ProviderDescriptor。"
      meta={<span>{providers.length} providers</span>}
    >
      <div className="table-scroll">
        <table className="phase1-table">
          <thead>
            <tr>
              <th>Provider</th>
              <th>协议</th>
              <th>Streaming</th>
              <th>Tools</th>
              <th>Structured</th>
              <th>Embedding</th>
            </tr>
          </thead>
          <tbody>
            {providers.map((provider) => (
              <tr key={provider.id}>
                <td>
                  <strong>{provider.id}</strong>
                </td>
                <td>{provider.protocols.join(", ")}</td>
                <td>{booleanCapability(provider.supports_streaming)}</td>
                <td>{booleanCapability(provider.supports_tools)}</td>
                <td>
                  {booleanCapability(provider.supports_structured_output)}
                </td>
                <td>{booleanCapability(provider.supports_embeddings)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </DataSurface>
  );
}

function booleanCapability(value: boolean) {
  return value ? (
    <span className="boolean yes">
      <ShieldCheck size={13} /> yes
    </span>
  ) : (
    <span className="boolean no">no</span>
  );
}

function profileName(
  profiles: ModelProfileLifecycleView[],
  profileId: string,
) {
  return (
    profiles.find((profile) => profile.profile_id === profileId)
      ?.profile_name ?? shortId(profileId)
  );
}

function shortId(value: string) {
  return value.length > 12
    ? `${value.slice(0, 8)}…${value.slice(-4)}`
    : value;
}

function modelOperationError(error: unknown) {
  return error instanceof ApiError
    ? `${error.code} · ${error.message}`
    : "模型治理操作失败；状态未被乐观更新，请重新读取后重试。";
}
