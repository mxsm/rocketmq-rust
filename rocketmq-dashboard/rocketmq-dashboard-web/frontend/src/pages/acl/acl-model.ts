import type { AclPolicyEntryView, AclPolicyRequest, AclPolicyView, AclQueryParams, AclUserView } from '../../types/acl';
import type { BrokerInfo } from '../../types/broker';
import type { SelectMenuOption } from '../../components/SelectMenu';

export interface AclScope {
  clusterName: string;
  brokerName: string;
}

export interface AclScopeOptions {
  clusters: SelectMenuOption[];
  brokers: SelectMenuOption[];
}

export interface AclPolicyDraft {
  subject: string;
  policyType: string;
  resources: string;
  actions: string[];
  sourceIps: string;
  decision: string;
}

export interface AclPolicyRow {
  key: string;
  entryIndex: number;
  subjectEntries: AclPolicyEntryView[];
  brokerName: string;
  brokerAddr: string;
  subject: string;
  policyType: string;
  resource: string;
  actions: string[];
  sourceIps: string[];
  decision: string;
}

export type AclPolicyDraftResult =
  | { ok: true; value: AclPolicyRequest }
  | { ok: false; errors: Partial<Record<keyof AclPolicyDraft, string>> };

export function deriveAclScopeOptions(brokers: BrokerInfo[], clusterName: string): AclScopeOptions {
  const clusters = uniqueSorted(brokers.map((broker) => broker.clusterName))
    .map((value) => ({ value, label: value }));
  const scopedBrokers = brokers.filter((broker) => broker.clusterName === clusterName);
  const brokersOptions = uniqueSorted(scopedBrokers.map((broker) => broker.brokerName))
    .map((value) => ({ value, label: value }));

  return { clusters, brokers: brokersOptions };
}

export function createAclScopeQuery(scope: AclScope | null, brokers: BrokerInfo[]): AclQueryParams | null {
  if (!scope || !scope.clusterName || !scope.brokerName) return null;

  const matchesBroker = brokers.some((broker) =>
    broker.clusterName === scope.clusterName && broker.brokerName === scope.brokerName
  );

  return matchesBroker ? { clusterName: scope.clusterName, brokerName: scope.brokerName } : null;
}

export function filterAclUsers(users: AclUserView[], query: string): AclUserView[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return [...users];

  return users.filter((user) => matchesQuery([
    user.username,
    user.brokerName,
    user.brokerAddr,
    user.userType,
    user.userStatus
  ], normalizedQuery));
}

export function filterAclPolicies(policies: AclPolicyView[], query: string): AclPolicyView[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return [...policies];

  return policies.filter((policy) => matchesQuery([
    policy.subject,
    policy.policyType,
    policy.brokerName,
    policy.brokerAddr,
    ...policy.entries.flatMap((entry) => [
      entry.resource,
      entry.decision,
      ...entry.actions,
      ...entry.sourceIps
    ])
  ], normalizedQuery));
}

export function flattenAclPolicies(policies: AclPolicyView[]): AclPolicyRow[] {
  return policies.flatMap((policy, policyIndex) => policy.entries.map((entry, entryIndex) => ({
    key: `${policy.brokerAddr}:${policy.subject ?? ''}:${policy.policyType ?? ''}:${entry.resource ?? '*'}:${policyIndex}:${entryIndex}`,
    entryIndex,
    subjectEntries: policy.entries.map((subjectEntry) => ({
      resource: subjectEntry.resource,
      actions: [...subjectEntry.actions],
      sourceIps: [...subjectEntry.sourceIps],
      decision: subjectEntry.decision
    })),
    brokerName: policy.brokerName,
    brokerAddr: policy.brokerAddr,
    subject: policy.subject ?? '-',
    policyType: policy.policyType ?? 'Custom',
    resource: entry.resource ?? '*',
    actions: [...entry.actions],
    sourceIps: [...entry.sourceIps],
    decision: entry.decision ?? 'Allow'
  })));
}

export function filterAclPolicyRows(rows: AclPolicyRow[], query: string): AclPolicyRow[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return [...rows];

  return rows.filter((row) => matchesQuery([
    row.subject,
    row.policyType,
    row.brokerName,
    row.brokerAddr,
    row.resource,
    row.decision,
    ...row.actions,
    ...row.sourceIps
  ], normalizedQuery));
}

export function buildAclPolicyRequest(scope: AclScope, draft: AclPolicyDraft, selectedPolicy?: AclPolicyRow | null): AclPolicyDraftResult {
  const subject = draft.subject.trim();
  const policyType = draft.policyType.trim();
  const resource = parseCommaSeparatedValues(draft.resources);
  const actions = draft.actions.map((action) => action.trim()).filter(Boolean);
  const sourceIps = parseCommaSeparatedValues(draft.sourceIps);
  const decision = draft.decision.trim();
  const errors: Partial<Record<keyof AclPolicyDraft, string>> = {};

  if (!subject) errors.subject = 'Subject is required.';
  if (!policyType) errors.policyType = 'Policy type is required.';
  if (resource.length === 0) errors.resources = 'At least one resource is required.';
  if (actions.length === 0) errors.actions = 'At least one action is required.';
  if (!decision) errors.decision = 'Decision is required.';

  if (Object.keys(errors).length > 0) return { ok: false, errors };

  const editedEntry = { resource, actions, sourceIps, decision };
  const entries = selectedPolicy
    ? selectedPolicy.subjectEntries.map((entry, entryIndex) => entryIndex === selectedPolicy.entryIndex
      ? editedEntry
      : {
          resource: parseCommaSeparatedValues(entry.resource ?? '*'),
          actions: [...entry.actions],
          sourceIps: [...entry.sourceIps],
          decision: entry.decision ?? 'Allow'
        })
    : [editedEntry];

  return {
    ok: true,
    value: {
      brokerName: scope.brokerName,
      clusterName: scope.clusterName,
      subject,
      policies: [{ policyType, entries }]
    }
  };
}

export function parseCommaSeparatedValues(value: string): string[] {
  return value.split(',').map((item) => item.trim()).filter(Boolean);
}

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean))).sort((left, right) => left.localeCompare(right));
}

function matchesQuery(values: Array<string | undefined>, normalizedQuery: string): boolean {
  return values.some((value) => value?.toLowerCase().includes(normalizedQuery));
}
