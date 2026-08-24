import type { StorageBackend, StorageMode } from './config';

export type StorageStatus = 'available' | 'degraded' | 'unavailable';
export type StorageStatusReason = 'capacityBelowReserve' | 'probeFailed';

/** Safe operator status; connection strings and filesystem locations are never included. */
export interface StorageStatusView {
  backend: StorageBackend;
  mode: StorageMode;
  status: StorageStatus;
  reason?: StorageStatusReason;
  schemaOrFormatVersion?: number;
  observationStartedAt: number;
  lastSuccessfulWriteAt?: number;
  safeAvailableBytes?: number;
  poolSize?: number;
  idleConnections?: number;
}
