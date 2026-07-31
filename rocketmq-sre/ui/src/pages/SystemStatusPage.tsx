import {
  Activity,
  Box,
  Braces,
  CheckCircle2,
  Database,
  Network,
  ShieldCheck,
} from "lucide-react";
import { useEffect, useState } from "react";

import type { CapabilityCatalogResponse } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { useSreData } from "@/data/SreDataContext";

export function SystemStatusPage() {
  const { health, readiness, catalog } = useSreData();
  const [capabilities, setCapabilities] =
    useState<CapabilityCatalogResponse>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    const controller = new AbortController();
    void catalog(controller.signal)
      .then(setCapabilities)
      .catch((cause: unknown) =>
        setError(cause instanceof Error ? cause.message : "能力目录不可用"),
      );
    return () => controller.abort();
  }, [catalog]);

  return (
    <div className="page">
      <PageHeader
        eyebrow="SYSTEM STATUS"
        title="系统状态"
        description="查看 Control Plane、持久化、协议边界和离线 Provider 能力描述。"
      />
      {error && <div className="inline-alert warning">{error}</div>}

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
          state="descriptor / fixture only"
          success
        />
      </section>

      <section className="data-surface provider-surface">
        <div className="surface-heading">
          <div>
            <h2>协议适配矩阵</h2>
            <p>
              Phase 00 仅提供 ProviderDescriptor，不发起真实模型网络调用。
            </p>
          </div>
          <Badge variant="outline">
            {capabilities?.providers.length ?? 0} providers
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
                <th>Phase 00</th>
              </tr>
            </thead>
            <tbody>
              {capabilities?.providers.map((provider) => (
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
                    <Badge variant="secondary">descriptor only</Badge>
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
