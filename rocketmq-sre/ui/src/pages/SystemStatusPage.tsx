import {
  Activity,
  Box,
  Braces,
  CheckCircle2,
  Database,
  Network,
  ShieldCheck,
} from "lucide-react";
import { useCallback } from "react";

import type { ModelProfile } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { formatTime } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { useSreData } from "@/data/SreDataContext";
import {
  modelGatewayPresentation,
  modelProfileHealthLabel,
  modelProfileHealthVariant,
} from "@/features/models/modelCapabilityPresentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";

export function SystemStatusPage() {
  const { health, readiness, catalog, api } = useSreData();
  const loadCatalog = useCallback(
    (signal: AbortSignal) => catalog(signal),
    [catalog],
  );
  const loadModelCapabilities = useCallback(
    (signal: AbortSignal) => api.getModelCapabilities(signal),
    [api],
  );
  const catalogResource = useAsyncResource(loadCatalog);
  const modelResource = useAsyncResource(loadModelCapabilities);
  const capabilities = catalogResource.data;
  const modelCapabilities = modelResource.data;
  const modelGateway = modelGatewayPresentation(
    modelCapabilities,
    Boolean(modelResource.error),
  );
  const providers =
    modelCapabilities?.providers ?? capabilities?.providers ?? [];

  return (
    <div className="page">
      <PageHeader
        eyebrow="SYSTEM STATUS"
        title="系统状态"
        description="查看 Control Plane、持久化、协议边界，以及 Model Gateway 的真实运行配置和健康状态。"
      />
      {Boolean(catalogResource.error) && (
        <div className="inline-alert warning">
          能力目录不可用；未返回的执行边界不会按已认证处理。
        </div>
      )}
      {Boolean(modelResource.error) && (
        <div className="inline-alert warning">
          Model Gateway 运行状态不可用；不会回退为 descriptor-only 或 healthy。
        </div>
      )}

      <section className="system-status-grid">
        <SystemItem
          icon={Activity}
          label="Control Plane"
          state={health === "healthy" ? "进程健康" : health}
          success={health === "healthy"}
        />
        <SystemItem
          icon={Database}
          label="PostgreSQL / migrations"
          state={readiness === "ready" ? "就绪" : readiness}
          success={readiness === "ready"}
        />
        <SystemItem
          icon={Network}
          label="MCP Protocol"
          state="2025-11-25"
          success
        />
        <SystemItem
          icon={Braces}
          label="Evidence Schema"
          state="rocketmq-sre.evidence.v1"
          success
        />
        <SystemItem
          icon={ShieldCheck}
          label="执行边界"
          state={
            capabilities?.execution_supported === false
              ? "execution_supported=false"
              : "未认证"
          }
          success={capabilities?.execution_supported === false}
        />
        <SystemItem
          icon={Box}
          label="Model Gateway"
          state={modelGateway.label}
          success={modelGateway.success}
        />
      </section>

      <section className="data-surface provider-surface">
        <div className="surface-heading">
          <div>
            <h2>运行时 Provider Profiles</h2>
            <p>
              仅展示脱敏 Profile 身份、健康和凭据是否已配置；不返回 token、secret 或 endpoint。
            </p>
          </div>
          <Badge
            variant={modelCapabilities?.network_calls_enabled ? "success" : "outline"}
          >
            {modelResource.loading
              ? "loading"
              : modelResource.error
                ? "unavailable"
                : modelCapabilities?.network_calls_enabled
                  ? "network enabled"
                  : "rules-only"}
          </Badge>
        </div>
        {modelCapabilities && modelCapabilities.profiles.length > 0 ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Profile</th>
                  <th>Provider / Model</th>
                  <th>协议</th>
                  <th>健康</th>
                  <th>凭据</th>
                  <th>最近观测</th>
                </tr>
              </thead>
              <tbody>
                {modelCapabilities.profiles.map((profile) => (
                  <tr key={profile.id}>
                    <td>
                      <div className="provider-profile-identity">
                        <strong>{profile.profile_name}</strong>
                        <span>{profile.region}</span>
                      </div>
                    </td>
                    <td>
                      <div className="provider-profile-identity">
                        <strong>{profile.provider_family}</strong>
                        <span>
                          {profile.model_family} · {profile.model_revision}
                        </span>
                      </div>
                    </td>
                    <td>{profile.protocol_family}</td>
                    <td>
                      <Badge variant={modelProfileHealthVariant(profile.health)}>
                        {modelProfileHealthLabel(profile.health)}
                      </Badge>
                    </td>
                    <td>
                      <CredentialStatus profile={profile} />
                    </td>
                    <td>{formatTime(profile.last_health_observed_at ?? undefined)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state compact">
            <strong>没有可用的运行时 Profile</strong>
            <span>
              {modelResource.loading
                ? "正在读取脱敏的运行时配置和健康状态。"
                : modelResource.error
                ? "运行状态查询失败；保持 fail closed。"
                : "Rules-only 仍可用；配置并认证 Profile 后才会启用模型网络调用。"}
            </span>
          </div>
        )}
      </section>

      <section className="data-surface provider-surface">
        <div className="surface-heading">
          <div>
            <h2>协议适配矩阵</h2>
            <p>
              描述已实现的协议适配能力；是否已配置、健康或进入路由以上方运行时 Profile 为准。
            </p>
          </div>
          <Badge variant="outline">
            {providers.length} providers
          </Badge>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Provider</th>
                <th>协议</th>
                <th>Streaming</th>
                <th>Tools</th>
                <th>Structured Output</th>
                <th>Embeddings</th>
                <th>适配状态</th>
              </tr>
            </thead>
            <tbody>
              {providers.map((provider) => (
                <tr key={provider.id}>
                  <td>
                    <strong>{providerName(provider.id)}</strong>
                  </td>
                  <td>{provider.protocols.join(", ")}</td>
                  <td>{booleanLabel(provider.supports_streaming)}</td>
                  <td>{booleanLabel(provider.supports_tools)}</td>
                  <td>
                    {booleanLabel(provider.supports_structured_output)}
                  </td>
                  <td>{booleanLabel(provider.supports_embeddings)}</td>
                  <td>
                    <Badge variant="secondary">adapter available</Badge>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function CredentialStatus({ profile }: { profile: ModelProfile }) {
  return profile.credential_configured ? (
    <Badge variant="success">凭据已配置</Badge>
  ) : (
    <Badge variant="outline">未配置凭据</Badge>
  );
}

function SystemItem({
  icon: Icon,
  label,
  state,
  success = false,
}: {
  icon: typeof Activity;
  label: string;
  state: string;
  success?: boolean;
}) {
  return (
    <article className="system-item">
      <Icon aria-hidden="true" size={18} />
      <span>{label}</span>
      <strong className={success ? "safe-text" : undefined}>
        {success && <CheckCircle2 aria-hidden="true" size={14} />}
        {state}
      </strong>
    </article>
  );
}

function providerName(id: string) {
  const names: Record<string, string> = {
    deepseek: "DeepSeek",
    "zhipu-glm": "智谱 GLM",
    "kimi-moonshot": "Kimi / Moonshot",
    "google-gemini": "Google Gemini",
    "aws-bedrock": "AWS Bedrock",
    "local-openai-compatible": "Local OpenAI-compatible",
  };
  return names[id] ?? id;
}

function booleanLabel(value: boolean) {
  return value ? "支持" : "不支持";
}
