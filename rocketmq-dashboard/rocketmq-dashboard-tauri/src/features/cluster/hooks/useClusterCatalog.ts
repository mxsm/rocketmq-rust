import { useCallback } from 'react';
import { ClusterService } from '../../../services/cluster.service';
import { useReadResource } from '../../../hooks/useReadResource';

const loadCluster = () => ClusterService.getClusterHomePage({ forceRefresh: false });

export function useClusterCatalog() {
    const state = useReadResource(loadCluster, 'Cluster data could not be read.');
    const refresh = useCallback(() => state.read(() => ClusterService.getClusterHomePage({ forceRefresh: true })), [state.read]);
    return { ...state, refresh };
}
