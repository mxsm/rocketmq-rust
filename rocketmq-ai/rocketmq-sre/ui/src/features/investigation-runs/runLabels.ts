import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

const labels: Record<string, [string, string]> = {
  queued: ["已排队", "Queued"],
  running: ["调查中", "Running"],
  pausing: ["暂停协调中", "Pausing"],
  paused: ["已暂停", "Paused"],
  needs_evidence: ["需要证据", "Needs evidence"],
  completed: ["已完成", "Completed"],
  inconclusive: ["结论不确定", "Inconclusive"],
  cancelling: ["取消协调中", "Cancelling"],
  cancelled: ["已取消", "Cancelled"],
  failed: ["失败", "Failed"],
  healthy: ["健康", "Healthy"],
  fault: ["发现异常", "Fault"],
  unsupported: ["不支持", "Unsupported"],
  available: ["可用", "Available"],
  partial: ["不完整", "Partial"],
  missing: ["缺失", "Missing"],
  stale: ["已过期", "Stale"],
  unknown: ["未知", "Unknown"],
  evidence_insufficient: ["证据不足", "Insufficient evidence"],
  budget_exhausted: ["共享预算不足", "Shared budget exhausted"],
  deadline_exceeded: ["超过截止时间", "Deadline exceeded"],
  no_progress: ["事实未产生进展", "No factual progress"],
  model_unavailable: [
    "模型不可用，保留规则结果",
    "Model unavailable; deterministic results retained",
  ],
  source_unavailable: ["所需数据源不可用", "Required source unavailable"],
  control_permission_required: [
    "当前身份没有控制权限",
    "Current identity lacks control permission",
  ],
  terminal: ["运行已终止", "Run is terminal"],
  state_not_eligible: [
    "当前状态不接受此命令",
    "Command unavailable in the current state",
  ],
  counter_exhausted: ["状态计数已达上限", "State counter exhausted"],
  pack_unavailable: [
    "所选诊断包不可用",
    "Selected diagnostic pack unavailable",
  ],
  scope_unavailable: ["调查范围不可用", "Investigation scope unavailable"],
  unsupported_resource: [
    "当前资源不支持运行调查",
    "Run investigations do not support this resource",
  ],
  worker_lease_expired: [
    "工作所有权过期；远端消耗未知",
    "Worker ownership expired; remote consumption unknown",
  ],
  owner_shutdown: [
    "服务关闭；未确认工作保留预约",
    "Service shutdown; unconfirmed work retains its reservation",
  ],
  source_outcome_unknown: [
    "只读源结果未知；预约保留",
    "Read outcome unknown; reservation retained",
  ],
  model_outcome_unknown: [
    "模型结果未知；预约保留",
    "Model outcome unknown; reservation retained",
  ],
  source_rejected: [
    "只读源拒绝请求，不自动重试",
    "Source rejected the request; no automatic retry",
  ],
  model_rejected: ["模型请求被拒绝", "Model request rejected"],
  invalid_input: ["请求不合法", "Invalid request"],
  created: ["已创建", "Created"],
  command_accepted: ["控制请求已接受", "Command accepted"],
  worker_claimed: ["工作所有权已获取", "Worker ownership acquired"],
  worker_released: ["工作所有权已释放", "Worker ownership released"],
  started: ["运行已开始", "Run started"],
  step_admitted: ["步骤已准入并预约预算", "Step admitted and budget reserved"],
  step_settled: ["步骤已结算", "Step settled"],
  attempt_interrupted: [
    "尝试中断，消耗未确认",
    "Attempt interrupted; consumption unconfirmed",
  ],
  assessment_recorded: ["结论已保存", "Assessment saved"],
  stopped: ["运行已停止", "Run stopped"],
  instant_observation_outside_requested_window: [
    "瞬时观测在请求时间窗外",
    "Instant observation is outside the requested window",
  ],
  source_temporal_contract_unknown: [
    "数据源时间覆盖未知",
    "Source time coverage unknown",
  ],
  required_fields_observed: ["必需字段已观测", "Required fields observed"],
};

export function useRunLabel() {
  const { locale } = useOperatorPreferences();
  return (value: string) =>
    labels[value]?.[locale === "en-US" ? 1 : 0] ??
    (/^[a-z0-9_.-]{1,96}$/iu.test(value)
      ? value
      : labels.unknown[locale === "en-US" ? 1 : 0]);
}
