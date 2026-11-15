import type { ActionReadinessView } from "@/api/types";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

type Reason = ActionReadinessView["stages"]["descriptor"]["reason_codes"][number];
type Source = ActionReadinessView["stages"]["descriptor"]["source"];

const zh: Record<Reason, string> = {
  action_unknown: "此动作未在服务端动作目录中注册。",
  descriptor_version_mismatch: "所选版本与服务端动作定义不一致。",
  manual_only: "此项只支持手工作业方案。",
  plan_only: "此项只支持计划草稿。",
  parameters_invalid: "动作参数不符合当前版本的约束。",
  resource_required: "请先指定动作目标。",
  resource_invalid: "目标不符合此动作的资源约束。",
  operator_required: "当前身份缺少此操作所需的操作权限。",
  diagnosis_not_confirmed: "当前诊断依据尚未经人工确认。",
  diagnosis_already_confirmed: "当前诊断已确认，无需重复确认。",
  diagnosis_not_current: "已有更新的诊断修订，请复核新修订。",
  incident_inactive: "此事件已不处于允许继续操作的状态。",
  diagnosis_incomplete: "当前诊断或其证据不完整。",
  primary_model_lineage_invalid: "当前修订缺少有效的模型调用来源。",
  executor_not_configured: "当前部署未配置隔离执行服务。",
  handler_not_observed: "尚未观测到此版本动作的实际处理器。",
  handler_read_observed: "实际处理器已接受此候选的只读预检。",
  runtime_enablement_not_observed: "尚未确认目标部署中的动作开关。",
  target_support_not_observed: "尚未取得此目标支持动作的有效观测。",
  target_precondition_not_ready: "目标当前未满足动作前置条件。",
  target_unsupported: "目标的已知能力不支持此动作。",
  target_scope_unverified: "尚未确认此目标在当前事件动作的授权范围内。",
  target_scope_changed: "目标读取范围的授权声明已变化，请重新采集前置证据。",
  evidence_missing: "缺少此候选所需的保存证据。",
  evidence_stale: "此候选绑定的原证据已超过时效限制。",
  evidence_invalid: "保存证据未通过完整性检查。",
  evidence_scope_mismatch: "保存证据与此候选的授权范围不一致。",
  preparation_binding_missing: "需要重新采集与当前参数绑定的只读前置证据。",
  preparation_binding_mismatch: "前置证据属于不同的参数、修订或操作身份。",
  preparation_expired: "此候选的前置证据已过期。",
  preparation_origin_invalid: "保存的前置证据来源无法验证，请重新采集。",
  policy_denied: "当前候选未通过服务端策略检查。",
  maintenance_window_closed: "当前不在允许的维护窗口内。",
  resource_quarantined: "目标处于隔离状态，需要人工处理。",
  resource_busy: "目标已有进行中的变更。",
  compensation_unavailable: "此动作缺少所需的补偿方式。",
  compensation_not_observed: "尚未实际确认此目标的补偿能力。",
  plan_not_created: "创建不可变计划后才进入审批。",
  approval_required: "此计划需要独立审批人的有效批准。",
  approval_expired: "原计划审批已过期。",
  approval_binding_mismatch: "原审批与当前计划、证据或策略不一致。",
  self_approval_forbidden: "计划发起人不能审批自己的计划。",
  action_target_qualification_unavailable: "尚无此动作版本与目标范围的独立资格记录。",
  observation_available: "已取得适用于当前候选的观测。",
  plan_expired: "此不可变计划已过期。",
  plan_binding_mismatch: "所选候选与保存计划的绑定不一致。",
  plan_completed: "计划已进入终态，保留历史结果。",
  plan_immutable: "当前计划不可变；如需更改预检或参数，请在事件中准备新候选。",
  readiness_expired: "准备状态已过期，需要重新读取。",
  source_unavailable: "所需状态来源暂不可用。",
};

const en: Record<Reason, string> = {
  action_unknown: "This action is not registered in the server action catalog.",
  descriptor_version_mismatch: "The selected version does not match the server action definition.",
  manual_only: "This item supports a manual runbook only.",
  plan_only: "This item supports a plan draft only.",
  parameters_invalid: "The parameters do not meet this action version's constraints.",
  resource_required: "Specify an action target first.",
  resource_invalid: "The target does not meet this action's resource constraints.",
  operator_required: "The current identity lacks the required operation permission.",
  diagnosis_not_confirmed: "The current diagnosis evidence has not been confirmed by an operator.",
  diagnosis_already_confirmed: "The current diagnosis is already confirmed and does not need another confirmation.",
  diagnosis_not_current: "A newer diagnosis revision is available. Review the new revision.",
  incident_inactive: "The incident is no longer in a state that permits this operation.",
  diagnosis_incomplete: "The current diagnosis or its evidence is incomplete.",
  primary_model_lineage_invalid: "The current revision has no valid primary model invocation lineage.",
  executor_not_configured: "The isolated execution service is not configured in this deployment.",
  handler_not_observed: "The actual handler for this action version has not been observed.",
  handler_read_observed: "The actual handler accepted this candidate's read-only precheck.",
  runtime_enablement_not_observed: "Action enablement in the target deployment has not been established.",
  target_support_not_observed: "No valid observation establishes action support for this target.",
  target_precondition_not_ready: "The target does not currently meet the action preconditions.",
  target_unsupported: "The target's observed capabilities do not support this action.",
  target_scope_unverified: "This target has not been established within the incident action's authorized scope.",
  target_scope_changed: "The target-read authorization declaration changed. Collect new precondition evidence.",
  evidence_missing: "Saved evidence required for this candidate is missing.",
  evidence_stale: "The original evidence bound to this candidate is stale.",
  evidence_invalid: "Saved evidence failed its integrity checks.",
  evidence_scope_mismatch: "Saved evidence does not match this candidate's authorized scope.",
  preparation_binding_missing: "Collect a new read-only precondition proof bound to the current parameters.",
  preparation_binding_mismatch: "The precondition proof belongs to different parameters, revision or operator identity.",
  preparation_expired: "This candidate's precondition proof expired.",
  preparation_origin_invalid: "The saved precondition evidence origin could not be verified. Collect it again.",
  policy_denied: "The current candidate did not pass server policy checks.",
  maintenance_window_closed: "The permitted maintenance window is closed.",
  resource_quarantined: "The target is quarantined and requires human intervention.",
  resource_busy: "A change is already in progress on this target.",
  compensation_unavailable: "The action lacks its required compensation method.",
  compensation_not_observed: "Compensation capability has not been established for this target.",
  plan_not_created: "Approval begins after an immutable plan is created.",
  approval_required: "This plan requires valid approval from an independent approver.",
  approval_expired: "The previous plan approval expired.",
  approval_binding_mismatch: "The previous approval does not match the current plan, evidence or policy.",
  self_approval_forbidden: "The plan author cannot approve their own plan.",
  action_target_qualification_unavailable: "No independent qualification record covers this action version and target scope.",
  observation_available: "An observation applies to the current candidate.",
  plan_expired: "This immutable plan expired.",
  plan_binding_mismatch: "The selected candidate does not match the saved plan binding.",
  plan_completed: "The plan is terminal and retains its historical result.",
  plan_immutable: "This Plan is immutable. Prepare a new candidate from the Incident to change its precheck or parameters.",
  readiness_expired: "Readiness expired. Read it again.",
  source_unavailable: "A required state source is temporarily unavailable.",
};

const sources: Record<Source, [string, string]> = {
  descriptor_catalog: ["动作目录", "Action catalog"],
  executor_configuration: ["当前部署", "Current deployment"],
  stored_preparation: ["保存的只读预检", "Saved read-only precheck"],
  current_diagnosis: ["当前诊断", "Current diagnosis"],
  policy_evaluation: ["服务端策略", "Server policy"],
  stored_approval: ["保存的计划审批", "Saved plan approval"],
  qualification_unavailable: ["资格来源缺失", "Qualification source unavailable"],
  target_scope_configuration: ["目标范围授权声明", "Target-scope authorization declaration"],
  not_observed: ["尚未观测", "Not observed"],
};

export function isReadinessReason(value: unknown): value is Reason {
  return typeof value === "string" && Object.hasOwn(zh, value);
}

export function isReadinessSource(value: unknown): value is Source {
  return typeof value === "string" && Object.hasOwn(sources, value);
}

export function useActionReadinessLabels() {
  const english = useOperatorPreferences().locale === "en-US";
  return {
    reason: (value: Reason) => (english ? en : zh)[value],
    source: (value: Source) => sources[value][english ? 1 : 0],
  };
}
