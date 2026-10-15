import { apiClient } from './client';
import type { TopologySnapshot } from '../types/topology';

export const topologyApi = {
  /** Every layer of the map from one backend query. Older backends answer `API_ROUTE_NOT_FOUND`. */
  snapshot: () => apiClient.get<TopologySnapshot>('/api/topology?mode=nameServer&skipSystem=true')
};
