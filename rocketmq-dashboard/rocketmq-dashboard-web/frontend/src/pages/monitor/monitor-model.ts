import type { ConsumerMonitorUpsertRequest, ConsumerMonitorView } from '../../types/monitor';

export type ConsumerMonitorDraftRequest = Omit<ConsumerMonitorUpsertRequest, 'environmentId' | 'expectedRevision'>;

export interface ConsumerMonitorDraft {
  consumerGroup: string;
  minCount: string;
  maxDiffTotal: string;
}

export type ConsumerMonitorDraftResult =
  | { ok: true; value: ConsumerMonitorDraftRequest }
  | { ok: false; errors: Partial<Record<keyof ConsumerMonitorDraft, string>> };

export interface ConsumerMonitorMetrics {
  ruleCount: number;
  minCountRange: string;
  maxDiffTotalRange: string;
}

export function parseConsumerMonitorDraft(draft: ConsumerMonitorDraft): ConsumerMonitorDraftResult {
  const errors: Partial<Record<keyof ConsumerMonitorDraft, string>> = {};
  const consumerGroup = draft.consumerGroup.trim();
  const minCount = parseNonNegativeInteger(draft.minCount);
  const maxDiffTotal = parseNonNegativeInteger(draft.maxDiffTotal);

  if (!consumerGroup) errors.consumerGroup = 'Group is required.';
  if (minCount === null) errors.minCount = 'Min Count must be a non-negative integer.';
  if (maxDiffTotal === null) errors.maxDiffTotal = 'Max Diff Total must be a non-negative integer.';

  if (Object.keys(errors).length > 0) return { ok: false, errors };

  return {
    ok: true,
    value: { consumerGroup, minCount: minCount!, maxDiffTotal: maxDiffTotal! }
  };
}

export function getConsumerMonitorMetrics(rows: ConsumerMonitorView[]): ConsumerMonitorMetrics {
  return {
    ruleCount: rows.length,
    minCountRange: getRange(rows.map((row) => row.minCount)),
    maxDiffTotalRange: getRange(rows.map((row) => row.maxDiffTotal))
  };
}

function parseNonNegativeInteger(value: string) {
  if (!/^\d+$/.test(value.trim())) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function getRange(values: number[]) {
  if (values.length === 0) return '—';
  const min = Math.min(...values);
  const max = Math.max(...values);
  return min === max ? String(min) : `${min}–${max}`;
}
