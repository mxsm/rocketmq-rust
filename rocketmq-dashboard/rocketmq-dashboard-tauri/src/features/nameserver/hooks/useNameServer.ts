import { useEffect, useState } from 'react';
import { NameServerService } from '../../../services/nameserver.service';

export const useNameServer = () => {
    const [nameServers, setNameServers] = useState<string[]>([]);
    const [selectedNameServer, setSelectedNameServer] = useState('');
    const [isVIPChannel, setIsVIPChannel] = useState(true);
    const [useTLS, setUseTLS] = useState(false);
    const [isLoading, setIsLoading] = useState(true);
    const [pendingAction, setPendingAction] = useState<string | null>(null);

    const loadNameServerSettings = async () => {
        setIsLoading(true);

        try {
            const response = await NameServerService.getHomePage();

            if (!response.success) {
                return {
                    success: false,
                    message: response.message || 'Failed to load NameServer settings',
                };
            }

            setNameServers(response.namesrvAddrList);
            setSelectedNameServer(response.currentNamesrv ?? '');
            setIsVIPChannel(response.useVIPChannel);
            setUseTLS(response.useTLS);

            return {
                success: true,
                message: response.message,
            };
        } catch (error) {
            return {
                success: false,
                message: error instanceof Error ? error.message : 'Failed to load NameServer settings',
            };
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void loadNameServerSettings();
    }, []);

    const addNameServer = async (address: string) => {
        setPendingAction('add');
        try {
            const response = await NameServerService.addNameServer(address);
            if (!response.success) {
                return response;
            }

            const refreshResult = await loadNameServerSettings();
            return refreshResult.success ? response : refreshResult;
        } finally {
            setPendingAction(null);
        }
    };

    const switchNameServer = async (address: string) => {
        setPendingAction(`switch:${address}`);
        try {
            const response = await NameServerService.switchNameServer(address);
            if (!response.success) {
                return response;
            }

            const refreshResult = await loadNameServerSettings();
            return refreshResult.success ? response : refreshResult;
        } finally {
            setPendingAction(null);
        }
    };

    const deleteNameServer = async (address: string) => {
        setPendingAction(`delete:${address}`);
        try {
            const response = await NameServerService.deleteNameServer(address);
            if (!response.success) {
                return response;
            }

            const refreshResult = await loadNameServerSettings();
            return refreshResult.success ? response : refreshResult;
        } finally {
            setPendingAction(null);
        }
    };

    const updateVIPChannel = async (enabled: boolean) => {
        const previousValue = isVIPChannel;
        setPendingAction('vip');
        setIsVIPChannel(enabled);

        try {
            const response = await NameServerService.updateVIPChannel(enabled);
            if (!response.success) {
                setIsVIPChannel(previousValue);
            }
            return response;
        } catch (error) {
            setIsVIPChannel(previousValue);
            return {
                success: false,
                message: error instanceof Error ? error.message : 'Failed to update VIP Channel',
            };
        } finally {
            setPendingAction(null);
        }
    };

    const updateUseTLS = async (enabled: boolean) => {
        const previousValue = useTLS;
        setPendingAction('tls');
        setUseTLS(enabled);

        try {
            const response = await NameServerService.updateUseTLS(enabled);
            if (!response.success) {
                setUseTLS(previousValue);
            }
            return response;
        } catch (error) {
            setUseTLS(previousValue);
            return {
                success: false,
                message: error instanceof Error ? error.message : 'Failed to update TLS',
            };
        } finally {
            setPendingAction(null);
        }
    };

    return {
        nameServers,
        selectedNameServer,
        isVIPChannel,
        useTLS,
        isLoading,
        pendingAction,
        reload: loadNameServerSettings,
        addNameServer,
        switchNameServer,
        deleteNameServer,
        updateVIPChannel,
        updateUseTLS,
    };
};
