import type {
  ModelProfileLifecycleState,
  ModelProfileLifecycleView,
} from "@/api/types";

export const lifecycleLabels: Record<ModelProfileLifecycleState, string> = {
  draft: "草稿",
  certified: "已认证",
  promoted: "生产路由",
  quarantined: "已隔离",
  retired: "已退役",
};

export const transitionLabels: Record<
  ModelProfileLifecycleState,
  string
> = {
  draft: "转为草稿",
  certified: "认证",
  promoted: "提升为生产",
  quarantined: "隔离",
  retired: "退役",
};

const transitions: Record<
  ModelProfileLifecycleState,
  ModelProfileLifecycleState[]
> = {
  draft: ["certified", "quarantined", "retired"],
  certified: ["promoted", "quarantined", "retired"],
  promoted: ["quarantined", "retired"],
  quarantined: ["certified", "retired"],
  retired: [],
};

export function allowedLifecycleTransitions(
  state: ModelProfileLifecycleState,
) {
  return transitions[state];
}

export function lifecycleBadgeVariant(
  state: ModelProfileLifecycleState,
): "success" | "warning" | "secondary" | "destructive" {
  switch (state) {
    case "promoted":
      return "success";
    case "certified":
      return "secondary";
    case "draft":
      return "warning";
    case "quarantined":
      return "destructive";
    case "retired":
      return "secondary";
  }
}

export function hasPassingSmoke(profile: ModelProfileLifecycleView) {
  return profile.latest_smoke?.overall_ok === true;
}

export function canGovernModels(roles: string[]) {
  return roles.some((role) =>
    ["model-governance", "rocketmq:model-governance", "sre-admin"].includes(
      role,
    ),
  );
}

export function summarizeModelGovernance(
  profiles: ModelProfileLifecycleView[],
) {
  return {
    promoted: profiles.filter((profile) => profile.state === "promoted")
      .length,
    healthy: profiles.filter(hasPassingSmoke).length,
    quarantined: profiles.filter(
      (profile) => profile.state === "quarantined",
    ).length,
    automationEligible: profiles.filter(
      (profile) => profile.automation_eligible,
    ).length,
  };
}
