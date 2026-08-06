import type {
  ModelCapabilitiesResponse,
  ModelProfile,
} from "@/api/types";

export interface ModelGatewayPresentation {
  label: string;
  success: boolean;
}

export function modelGatewayPresentation(
  capabilities: ModelCapabilitiesResponse | undefined,
  unavailable = false,
): ModelGatewayPresentation {
  if (unavailable) {
    return { label: "运行状态不可用", success: false };
  }
  if (!capabilities) {
    return { label: "正在读取运行状态", success: false };
  }
  if (!capabilities.network_calls_enabled) {
    return capabilities.rules_only_available
      ? { label: "Rules-only · Provider 网络关闭", success: true }
      : { label: "Provider 网络关闭", success: false };
  }
  const healthyProfile = capabilities.profiles.some(
    (profile) =>
      profile.health === "healthy" && profile.credential_configured,
  );
  return healthyProfile
    ? { label: "Provider 网络已启用", success: true }
    : { label: "Provider 网络已启用 · 无健康 Profile", success: false };
}

export function modelProfileHealthLabel(health: ModelProfile["health"]) {
  switch (health) {
    case "healthy":
      return "健康";
    case "degraded":
      return "已降级";
    case "quarantined":
      return "已隔离";
    case "disabled":
      return "已禁用";
    case "unknown":
      return "未验证";
  }
}

export function modelProfileHealthVariant(
  health: ModelProfile["health"],
): "success" | "warning" | "destructive" | "secondary" {
  switch (health) {
    case "healthy":
      return "success";
    case "degraded":
    case "unknown":
      return "warning";
    case "quarantined":
      return "destructive";
    case "disabled":
      return "secondary";
  }
}
