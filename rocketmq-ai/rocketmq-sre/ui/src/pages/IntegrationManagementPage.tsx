import {
  ArrowUpRight,
  Cable,
  CheckCircle2,
  CircleOff,
  KeyRound,
  Plus,
  RefreshCw,
  Send,
  TicketCheck,
} from "lucide-react";
import {
  type FormEvent,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Link } from "react-router-dom";

import { createReleaseManagementApi } from "@/api/releaseManagementClient";
import type {
  IntegrationAdapterKind,
  IntegrationDelivery,
  IntegrationDescriptor,
  IntegrationTargetView,
  RegisterIntegrationTargetRequest,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSreData } from "@/data/SreDataContext";
import { createMockReleaseManagementApi } from "@/data/phase3ReleaseDemo";
import {
  ChangeClusterSelect,
  ChangeStatePanel,
  ChangeWorkspaceNav,
} from "@/features/change-management/ChangeWorkspace";
import {
  DeliveryStatusBadge,
  ReleaseSafetyBanner,
} from "@/features/release-management/ReleaseWorkspace";
import {
  adapterKindLabel,
  formatReleaseTime,
  integrationEventLabel,
} from "@/features/release-management/releasePresentation";

const defaultEvents: RegisterIntegrationTargetRequest["outbound_events"] =
  [
    "plan_submitted",
    "approval_changed",
    "release_started",
    "release_paused",
    "release_rolling_back",
    "release_completed",
    "manual_takeover_required",
  ];

interface TargetDraft {
  descriptorId: string;
  name: string;
  endpoint: string;
  secretReference: string;
}

const emptyDraft: TargetDraft = {
  descriptorId: "",
  name: "",
  endpoint: "",
  secretReference: "",
};

export function IntegrationManagementPage() {
  const auth = useAuth();
  const { clusters, demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockReleaseManagementApi(auth.requestContext)
          : createReleaseManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [clusterId, setClusterId] = useState("");
  const [descriptors, setDescriptors] = useState<
    IntegrationDescriptor[]
  >([]);
  const [targets, setTargets] = useState<IntegrationTargetView[]>([]);
  const [deliveries, setDeliveries] = useState<IntegrationDelivery[]>(
    [],
  );
  const [draft, setDraft] = useState<TargetDraft>(emptyDraft);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    if (!clusterId && clusters[0]) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  useEffect(() => {
    if (!api || !clusterId) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void Promise.all([
      api.listIntegrationDescriptors(controller.signal),
      api.listIntegrationTargets(
        clusterId,
        undefined,
        undefined,
        200,
        controller.signal,
      ),
      api.listIntegrationDeliveries(
        clusterId,
        undefined,
        200,
        controller.signal,
      ),
    ])
      .then(([descriptorItems, targetPage, deliveryPage]) => {
        setDescriptors(descriptorItems);
        setTargets(targetPage.items);
        setDeliveries(deliveryPage.items);
        setDraft((current) => ({
          ...current,
          descriptorId:
            current.descriptorId || descriptorItems[0]?.id || "",
        }));
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "集成管理数据暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, clusterId, reloadKey]);

  const selectedDescriptor = descriptors.find(
    (item) => item.id === draft.descriptorId,
  );

  const registerTarget = async (event: FormEvent) => {
    event.preventDefault();
    if (
      !api ||
      !clusterId ||
      !selectedDescriptor ||
      !isAdapterKind(selectedDescriptor.integration_kind)
    ) {
      setError("请选择可用的版本化集成描述符。");
      return;
    }
    setSubmitting(true);
    setError(undefined);
    setMessage(undefined);
    const request: RegisterIntegrationTargetRequest = {
      cluster_id: clusterId,
      descriptor_id: selectedDescriptor.id,
      descriptor_version: selectedDescriptor.version,
      name: draft.name.trim(),
      adapter_kind: selectedDescriptor.integration_kind,
      endpoint: draft.endpoint.trim(),
      secret_reference: draft.secretReference.trim() || null,
      notification_target_id: null,
      enabled: true,
      inbound_approval: selectedDescriptor.inbound,
      outbound_events: defaultEvents,
    };
    try {
      await api.registerIntegrationTarget(request);
      setDraft({
        ...emptyDraft,
        descriptorId: selectedDescriptor.id,
      });
      setMessage("集成目标已登记；Secret 只保存引用，不进入页面或日志。");
      setReloadKey((current) => current + 1);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "集成目标登记失败",
      );
    } finally {
      setSubmitting(false);
    }
  };

  const toggleTarget = async (target: IntegrationTargetView) => {
    if (!api) {
      return;
    }
    setError(undefined);
    try {
      await api.setIntegrationTargetState(target.id, {
        enabled: !target.enabled,
      });
      setMessage(
        `${target.name} 已${target.enabled ? "禁用" : "启用"}。`,
      );
      setReloadKey((current) => current + 1);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "集成目标状态更新失败",
      );
    }
  };

  return (
    <div className="page change-page release-page">
      <PageHeader
        eyebrow="P3-12 · ITSM / CHATOPS / PAGER"
        title="外部协作集成"
        description="管理版本化 adapter、集群范围、outbox 投递和外部审批入口。Secret、token 与目标凭据不会显示在工作台。"
        actions={
          <div className="release-header-actions">
            <ChangeClusterSelect
              clusters={clusters}
              value={clusterId}
              onValueChange={setClusterId}
            />
            <Button
              aria-label="刷新集成状态"
              onClick={() => setReloadKey((current) => current + 1)}
              size="icon"
              variant="outline"
            >
              <RefreshCw aria-hidden="true" size={15} />
            </Button>
          </div>
        }
      />
      <ChangeWorkspaceNav />
      <ReleaseSafetyBanner />

      {loading && (
        <ChangeStatePanel
          state="loading"
          title="正在读取集成台账"
          detail="同时查询 descriptor、目标和 outbox 投递。"
        />
      )}
      {error && (
        <ChangeStatePanel
          state="error"
          title="集成管理暂不可用"
          detail={error}
        />
      )}
      {message && (
        <div className="form-message success" role="status">
          {message}
        </div>
      )}

      <section className="integration-descriptor-strip">
        {descriptors.map((descriptor) => (
          <article key={descriptor.id}>
            <span className="integration-icon">
              {descriptor.inbound ? (
                <TicketCheck aria-hidden="true" size={17} />
              ) : (
                <Send aria-hidden="true" size={17} />
              )}
            </span>
            <div>
              <strong>
                {isAdapterKind(descriptor.integration_kind)
                  ? adapterKindLabel(descriptor.integration_kind)
                  : descriptor.integration_kind}
              </strong>
              <small>
                {descriptor.version} · {descriptor.owner}
              </small>
            </div>
            <Badge variant={descriptor.inbound ? "info" : "outline"}>
              {descriptor.inbound ? "IN + OUT" : "OUTBOUND"}
            </Badge>
          </article>
        ))}
      </section>

      <div className="integration-workspace">
        <section className="integration-target-panel">
          <header>
            <div>
              <span className="section-kicker">REGISTERED TARGETS</span>
              <h2>集群集成目标</h2>
              <p>页面只展示能力与投递状态，不展示 endpoint 或 Secret 引用。</p>
            </div>
            <Badge variant="secondary">{targets.length} TARGETS</Badge>
          </header>
          <div className="integration-target-list">
            {targets.map((target) => (
              <article key={target.id}>
                <span
                  className={`target-state-icon ${target.enabled ? "enabled" : "disabled"}`}
                >
                  {target.enabled ? (
                    <CheckCircle2 aria-hidden="true" size={16} />
                  ) : (
                    <CircleOff aria-hidden="true" size={16} />
                  )}
                </span>
                <div className="integration-target-copy">
                  <header>
                    <strong>{target.name}</strong>
                    <Badge
                      variant={target.enabled ? "success" : "secondary"}
                    >
                      {target.enabled ? "ENABLED" : "DISABLED"}
                    </Badge>
                  </header>
                  <span>
                    {adapterKindLabel(target.adapter_kind)} · v
                    {target.descriptor_version}
                  </span>
                  <footer>
                    {target.inbound_approval && (
                      <Badge variant="info">审批输入</Badge>
                    )}
                    <Badge variant="outline">
                      {target.outbound_events.length} events
                    </Badge>
                    <code>{target.id.slice(0, 8)}</code>
                  </footer>
                </div>
                <Button
                  onClick={() => void toggleTarget(target)}
                  size="sm"
                  variant="outline"
                >
                  {target.enabled ? "禁用" : "启用"}
                </Button>
              </article>
            ))}
            {!loading && targets.length === 0 && (
              <ChangeStatePanel
                state="empty"
                title="尚未登记集成目标"
                detail="使用右侧表单登记版本化 adapter。"
              />
            )}
          </div>
        </section>

        <section className="integration-composer-panel">
          <header>
            <span className="composer-icon">
              <Plus aria-hidden="true" size={17} />
            </span>
            <div>
              <span className="section-kicker">VERSIONED ADAPTER</span>
              <h2>登记集成目标</h2>
              <p>只接受结构化字段和 Secret 引用；不接受凭据正文。</p>
            </div>
          </header>
          <form className="change-form" onSubmit={registerTarget}>
            <label className="span-2">
              <span>Adapter descriptor</span>
              <Select
                value={draft.descriptorId}
                onValueChange={(value) =>
                  setDraft((current) => ({
                    ...current,
                    descriptorId: value,
                  }))
                }
              >
                <SelectTrigger aria-label="Adapter descriptor">
                  <SelectValue placeholder="选择版本化 adapter" />
                </SelectTrigger>
                <SelectContent>
                  {descriptors.map((descriptor) => (
                    <SelectItem key={descriptor.id} value={descriptor.id}>
                      {descriptor.integration_kind} · {descriptor.version}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>
            <label className="span-2">
              <span>目标名称</span>
              <input
                maxLength={128}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    name: event.target.value,
                  }))
                }
                placeholder="例如：生产变更工单"
                required
                value={draft.name}
              />
            </label>
            <label className="span-2">
              <span>Adapter endpoint</span>
              <input
                maxLength={2048}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    endpoint: event.target.value,
                  }))
                }
                placeholder="https://itsm.example/sre/events"
                required
                type="url"
                value={draft.endpoint}
              />
            </label>
            <label className="span-2">
              <span>Secret 引用（可选，不是 Secret）</span>
              <span className="input-with-icon">
                <KeyRound aria-hidden="true" size={14} />
                <input
                  maxLength={512}
                  onChange={(event) =>
                    setDraft((current) => ({
                      ...current,
                      secretReference: event.target.value,
                    }))
                  }
                  placeholder="vault://rocketmq-sre/adapter/signing-key"
                  value={draft.secretReference}
                />
              </span>
            </label>
            <Button
              className="span-2"
              disabled={submitting}
              type="submit"
            >
              <Cable aria-hidden="true" size={15} />
              {submitting ? "正在登记…" : "登记集成目标"}
            </Button>
          </form>
        </section>
      </div>

      <section className="integration-delivery-panel">
        <header>
          <div>
            <span className="section-kicker">IDEMPOTENT OUTBOX</span>
            <h2>协作事件投递</h2>
            <p>重试只重放同一幂等事件，不会重复审批或执行。</p>
          </div>
          <Badge variant="outline">{deliveries.length} DELIVERIES</Badge>
        </header>
        <div className="integration-delivery-table" role="table">
          <div className="integration-delivery-head" role="row">
            <span>事件</span>
            <span>目标</span>
            <span>状态</span>
            <span>尝试</span>
            <span>时间</span>
            <span aria-label="详情" />
          </div>
          {deliveries.map((delivery) => (
            <div
              className="integration-delivery-row"
              key={delivery.id}
              role="row"
            >
              <div>
                <strong>
                  {integrationEventLabel(delivery.event_kind)}
                </strong>
                <span>{delivery.sanitized_summary}</span>
              </div>
              <code>{delivery.target_id.slice(0, 8)}</code>
              <DeliveryStatusBadge status={delivery.status} />
              <span>{delivery.attempt_count}</span>
              <time dateTime={delivery.created_at}>
                {formatReleaseTime(
                  delivery.delivered_at ??
                    delivery.next_attempt_at ??
                    delivery.created_at,
                )}
              </time>
              {delivery.deep_link.startsWith("/changes/") ? (
                <Button asChild size="icon" variant="ghost">
                  <Link
                    aria-label="打开关联发布"
                    to={delivery.deep_link}
                  >
                    <ArrowUpRight aria-hidden="true" size={15} />
                  </Link>
                </Button>
              ) : (
                <span />
              )}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function isAdapterKind(value: string): value is IntegrationAdapterKind {
  return [
    "mock_itsm",
    "signed_webhook_itsm",
    "chat_ops_webhook",
    "pager",
    "email",
  ].includes(value);
}
