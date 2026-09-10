import { useEffect, useRef, useState } from 'react';
import { NameServerService } from '../../../services/nameserver.service';
import type { ConnectionSettingsView } from '../../../services/connection.store';
import { dashboardErrorMessage } from '../../../services/invoke';
import type {
    NameServerConfigSnapshot,
    NameServerHomePageInfo,
    NameServerStatusItem,
} from '../types/nameserver.types';

const NAMESERVER_REFRESH_INTERVAL_MS = 5_000;

const buildServerStatuses = (
    snapshot: NameServerConfigSnapshot,
    previousServers: NameServerStatusItem[] = [],
): NameServerStatusItem[] => {
    const previousAliveByAddress = new Map(previousServers.map((server) => [server.address, server.isAlive]));

    return snapshot.namesrvAddrList.map((address) => ({
        address,
        isCurrent: snapshot.currentNamesrv === address,
        isAlive: previousAliveByAddress.get(address) ?? false,
    }));
};

export const useNameServer = () => {
    const [data, setData] = useState<NameServerHomePageInfo | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState('');
    const [pendingAction, setPendingAction] = useState<string | null>(null);
    const [newAddress, setNewAddress] = useState('');

    const loadedRevision = useRef<number | null>(null);
    const loadHomePage = async (poll = false) => {
        try {
            const homePage = await NameServerService.getHomePageInfo();
            if (poll && loadedRevision.current !== null && loadedRevision.current !== homePage.settings.revision) {
                setLoadError('Connection settings changed. Refresh and review your pending edits.');
                return homePage;
            }
            loadedRevision.current = homePage.settings.revision;
            setData(homePage);
            setLoadError('');
            return homePage;
        } catch (error) {
            const errorMessage = dashboardErrorMessage(error, 'NameServer operation failed');
            setLoadError(errorMessage);
            throw error;
        }
    };

    useEffect(() => {
        let isMounted = true;

        const loadInitialState = async () => {
            try {
                await loadHomePage();
            } catch (error) {
                if (isMounted) {
                    setLoadError(dashboardErrorMessage(error, 'NameServer operation failed'));
                }
            } finally {
                if (isMounted) {
                    setIsLoading(false);
                }
            }
        };

        void loadInitialState();

        const intervalId = window.setInterval(() => {
            void loadHomePage(true).catch(() => {});
        }, NAMESERVER_REFRESH_INTERVAL_MS);

        return () => {
            isMounted = false;
            window.clearInterval(intervalId);
        };
    }, []);

    const addNameServer = async () => {
        const nextAddress = newAddress.trim();
        if (!nextAddress) {
            throw new Error('Please enter a valid NameServer address');
        }

        setPendingAction('add');

        try {
            const result = await NameServerService.addNameServer(nextAddress, data?.settings.revision ?? -1);
            await loadHomePage();
            setNewAddress('');
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const switchNameServer = async (address: string) => {
        setPendingAction(`switch:${address}`);

        try {
            const result = await NameServerService.switchNameServer(address, data?.settings.revision ?? -1);
            await loadHomePage();
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const deleteNameServer = async (address: string) => {
        setPendingAction(`delete:${address}`);

        try {
            const result = await NameServerService.deleteNameServer(address, data?.settings.revision ?? -1);
            await loadHomePage();
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const updateSnapshot = (updater: (previous: NameServerHomePageInfo) => NameServerHomePageInfo) => {
        setData((previous) => (previous ? updater(previous) : previous));
    };

    const applySnapshot = (settings: ConnectionSettingsView) => {
        loadedRevision.current = settings.revision;
        const snapshot = settings.nameserver;
        setData((previous) => ({
            currentNamesrv: snapshot.currentNamesrv,
            namesrvAddrList: snapshot.namesrvAddrList,
            useVIPChannel: snapshot.useVIPChannel,
            useTLS: snapshot.useTLS,
            servers: buildServerStatuses(snapshot, previous?.servers),
            settings,
        }));
    };

    const updateVipChannel = async (enabled: boolean) => {
        if (!data) {
            return;
        }

        const previous = data;
        setPendingAction('vip');
        updateSnapshot((snapshot) => ({ ...snapshot, useVIPChannel: enabled }));

        try {
            const result = await NameServerService.updateVipChannel(enabled, data?.settings.revision ?? -1);
            applySnapshot(result.settings);
            return result.message;
        } catch (error) {
            setData(previous);
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const updateUseTls = async (enabled: boolean) => {
        if (!data) {
            return;
        }

        const previous = data;
        setPendingAction('tls');
        updateSnapshot((snapshot) => ({ ...snapshot, useTLS: enabled }));

        try {
            const result = await NameServerService.updateUseTls(enabled, data?.settings.revision ?? -1);
            applySnapshot(result.settings);
            return result.message;
        } catch (error) {
            setData(previous);
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    return {
        data,
        isLoading,
        loadError,
        newAddress,
        pendingAction,
        setNewAddress,
        loadHomePage,
        addNameServer,
        switchNameServer,
        deleteNameServer,
        updateVipChannel,
        updateUseTls,
    };
};
