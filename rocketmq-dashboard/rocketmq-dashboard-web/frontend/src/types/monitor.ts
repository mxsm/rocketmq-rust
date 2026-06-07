export interface ConsumerMonitorView {
  consumerGroup: string;
  minCount: number;
  maxDiffTotal: number;
}

export interface ConsumerMonitorUpsertRequest {
  consumerGroup: string;
  minCount: number;
  maxDiffTotal: number;
}

export interface ConsumerMonitorMutationResult {
  message: string;
  item: ConsumerMonitorView | null;
}
