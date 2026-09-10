import { invokeAuthenticatedCommand } from './invoke';
import type {
    ProxyHomePageInfo,
    ProxyMutationResult,
} from '../features/proxy/types/proxy.types';

export class ProxyService {
    static async getHomePageInfo(): Promise<ProxyHomePageInfo> {
        return invokeAuthenticatedCommand<ProxyHomePageInfo>('get_proxy_home_page');
    }

    static async addProxyAddr(address: string, expectedRevision: number): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('add_proxy_addr', { address, expectedRevision });
    }

    static async switchProxyAddr(address: string, expectedRevision: number): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('switch_proxy_addr', { address, expectedRevision });
    }

    static async deleteProxyAddr(address: string, expectedRevision: number): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('delete_proxy_addr', { address, expectedRevision });
    }
}
