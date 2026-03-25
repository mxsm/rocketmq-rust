import { useEffect, useState } from 'react';
import { NameServerService } from '../../../services/nameserver.service';
import type {
    NameServerConfigSnapshot,
    NameServerHomePageInfo,
    NameServerStatusItem,
} from '../types/nameserver.types';

const NAMESERVER_REFRESH_INTERVAL_MS = 5_000;

const toErrorMessage = (error: unknown) => {
    if (error instanceof Error) {
        return error.message;
    }

    if (typeof error === 'string') {
        return error;
    }

    return 'NameServer operation failed';
};

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

    const loadHomePage = async () => {
        try {
            const homePage = await NameServerService.getHomePageInfo();
            setData(homePage);
            setLoadError('');
            return homePage;
        } catch (error) {
            const errorMessage = toErrorMessage(error);
            setLoadError(errorMessage);
            throw new Error(errorMessage);
        }
    };

    useEffect(() => {
        let isMounted = true;

        const loadInitialState = async () => {
            try {
                await loadHomePage();
            } catch (error) {
                console.error('Failed to load NameServer home page', error);
                if (isMounted) {
                    setLoadError(toErrorMessage(error));
                }
            } finally {
                if (isMounted) {
                    setIsLoading(false);
                }
            }
        };

        void loadInitialState();

        const intervalId = window.setInterval(() => {
            void loadHomePage().catch((error) => {
                console.error('Failed to refresh NameServer home page', error);
            });
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
            const result = await NameServerService.addNameServer(nextAddress);
            await loadHomePage();
            setNewAddress('');
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingAction(null);
        }
    };

    const switchNameServer = async (address: string) => {
        setPendingAction(`switch:${address}`);

        try {
            const result = await NameServerService.switchNameServer(address);
            await loadHomePage();
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingAction(null);
        }
    };

    const deleteNameServer = async (address: string) => {
        setPendingAction(`delete:${address}`);

        try {
            const result = await NameServerService.deleteNameServer(address);
            await loadHomePage();
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingAction(null);
        }
    };

    const updateSnapshot = (updater: (previous: NameServerHomePageInfo) => NameServerHomePageInfo) => {
        setData((previous) => (previous ? updater(previous) : previous));
    };

    const applySnapshot = (snapshot: NameServerConfigSnapshot) => {
        setData((previous) => ({
            currentNamesrv: snapshot.currentNamesrv,
            namesrvAddrList: snapshot.namesrvAddrList,
            useVIPChannel: snapshot.useVIPChannel,
            useTLS: snapshot.useTLS,
            servers: buildServerStatuses(snapshot, previous?.servers),
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
            const result = await NameServerService.updateVipChannel(enabled);
            applySnapshot(result.snapshot);
            return result.message;
        } catch (error) {
            setData(previous);
            throw new Error(toErrorMessage(error));
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
            const result = await NameServerService.updateUseTls(enabled);
            applySnapshot(result.snapshot);
            return result.message;
        } catch (error) {
            setData(previous);
            throw new Error(toErrorMessage(error));
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
