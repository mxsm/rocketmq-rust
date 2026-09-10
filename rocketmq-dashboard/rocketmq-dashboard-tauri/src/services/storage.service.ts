import { invokeAuthenticatedCommand } from './invoke';

export interface StorageStatus {
    backend: 'sqlite';
    mode: 'singleNode';
    available: boolean;
    schemaVersion: number | null;
    observedSinceMs: number;
    checkedAtMs: number;
    lastWriteMs: number | null;
    databaseBytes: number | null;
    reusableBytes: number | null;
    diskFreeBytes: number | null;
    error: string | null;
}
export const StorageService = {
    status: (): Promise<StorageStatus> => invokeAuthenticatedCommand('get_storage_status'),
};
