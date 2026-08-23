export interface ConsumerMonitorView {
  environmentId: string;
  consumerGroup: string;
  minCount: number;
  maxDiffTotal: number;
  revision: number;
}

export interface ConsumerMonitorUpsertRequest {
  environmentId: string;
  consumerGroup: string;
  minCount: number;
  maxDiffTotal: number;
  expectedRevision: number;
}

export interface ConsumerMonitorMutationResult {
  message: string;
  item: ConsumerMonitorView | null;
}
