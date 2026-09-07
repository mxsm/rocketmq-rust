import { invokeAuthenticatedCommand } from './invoke';
import type {
    ProxyConfigSnapshot,
    ProxyMutationResult,
} from '../features/proxy/types/proxy.types';

export class ProxyService {
    static async getHomePageInfo(): Promise<ProxyConfigSnapshot> {
        return invokeAuthenticatedCommand<ProxyConfigSnapshot>('get_proxy_home_page');
    }

    static async addProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('add_proxy_addr', { address });
    }

    static async switchProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('switch_proxy_addr', { address });
    }

    static async deleteProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invokeAuthenticatedCommand<ProxyMutationResult>('delete_proxy_addr', { address });
    }
}
