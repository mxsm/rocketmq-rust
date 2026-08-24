import { apiClient } from './client';
import type { StorageStatusView } from '../types/ops';

export const opsApi = {
  getStorageStatus: () => apiClient.get<StorageStatusView>('/api/ops/storage/status')
};
