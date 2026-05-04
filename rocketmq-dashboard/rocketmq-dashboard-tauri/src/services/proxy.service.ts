import { invoke } from '@tauri-apps/api/core';
import type {
    ProxyConfigSnapshot,
    ProxyMutationResult,
} from '../features/proxy/types/proxy.types';

export class ProxyService {
    static async getHomePageInfo(): Promise<ProxyConfigSnapshot> {
        return invoke<ProxyConfigSnapshot>('get_proxy_home_page');
    }

    static async addProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invoke<ProxyMutationResult>('add_proxy_addr', { address });
    }

    static async switchProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invoke<ProxyMutationResult>('switch_proxy_addr', { address });
    }

    static async deleteProxyAddr(address: string): Promise<ProxyMutationResult> {
        return invoke<ProxyMutationResult>('delete_proxy_addr', { address });
    }
}
