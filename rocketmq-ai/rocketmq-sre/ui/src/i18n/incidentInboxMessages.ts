import { incidentStatusLabels } from "@/features/incidents/incidentPresentation";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

import { useWorkspaceMessages } from "./workspaceMessages";

const zh = {
  scope: "范围",
  allStatus: "全部状态",
  severity: "严重程度",
  allSeverity: "全部严重程度",
  owner: "负责人",
  allOwners: "全部负责人",
  unassigned: "未分派",
  anyDate: "全部日期",
  dates: "事件更新日期",
  search: "搜索事件",
  searchHint: "搜索事件、资源或负责人",
  matched: "个事件",
  empty: "当前没有事件",
  emptyHint: "可从调查或巡检建议升级为事件。",
  noMatch: "没有符合筛选条件的事件",
  clearHint: "请调整日期、状态、严重程度、负责人或关键词。",
  stale: "证据已过期",
  alerts: "合并告警",
  details: "查看事件",
  unknown: "未记录",
  symptom: "症状",
  noSummary: "等待诊断摘要。",
  severityLabels: {
    critical: "严重",
    error: "错误",
    warning: "警告",
    info: "提示",
  },
  statusLabels: incidentStatusLabels,
};
const en: typeof zh = {
  scope: "Scope",
  allStatus: "All statuses",
  severity: "Severity",
  allSeverity: "All severities",
  owner: "Owner",
  allOwners: "All owners",
  unassigned: "Unassigned",
  anyDate: "All dates",
  dates: "Incident update date",
  search: "Search incidents",
  searchHint: "Search incidents, resources or owners",
  matched: "incidents",
  empty: "No incidents yet",
  emptyHint:
    "An investigation or inspection recommendation can become an incident.",
  noMatch: "No incidents match these filters",
  clearHint: "Adjust the date, status, severity, owner or search.",
  stale: "Stale evidence",
  alerts: "Merged alerts",
  details: "View incident",
  unknown: "Unknown",
  symptom: "Symptom",
  noSummary: "Awaiting a diagnostic summary.",
  severityLabels: {
    critical: "Critical",
    error: "Error",
    warning: "Warning",
    info: "Info",
  },
  statusLabels: {
    new: "New",
    collecting: "Collecting",
    diagnosing: "Diagnosing",
    needs_evidence: "Needs evidence",
    monitoring: "Monitoring",
    resolved: "Resolved",
    escalated: "Escalated",
  },
};

export function useIncidentInboxMessages() {
  const { locale } = useOperatorPreferences();
  return { ...useWorkspaceMessages(), ...(locale === "en-US" ? en : zh) };
}
