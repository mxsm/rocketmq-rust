import { useState, useSyncExternalStore, type ReactNode } from 'react';
import { ArrowLeft } from 'lucide-react';
import { toast } from 'sonner';
import { Toaster } from '../../components/ui/sonner';
import { useAppStore } from '../../stores/app.store';
import { ConnectionStore } from '../../services/connection.store';
import { SignOutConfirmDialog, useAuth } from '../../features/auth';
import { AppSidebar } from './AppSidebar';
import { EnvironmentToolbar } from './EnvironmentToolbar';
import { PageHeadingActions, PageToolbarProvider } from './pageToolbar';
import { pageDescriptions } from './navigation';

export function MainLayout({ children }: { children: ReactNode }) {
    const { activeTab, pageTitle, navigation, canGoBack, goBack } = useAppStore();
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const { logout } = useAuth();
    const [confirmSignOut, setConfirmSignOut] = useState(false);
    const [signingOut, setSigningOut] = useState(false);
    const target = navigation.target;
    // Message and trace links prefill editable queries; their current identity lives in the page.
    const targetName = target && target.kind !== 'trace' && target.kind !== 'message' ? ('name' in target ? target.name : target.address) : null;
    const handleSignOut = async () => {
        if (signingOut) return;
        setSigningOut(true);
        try {
            await logout();
            toast.success('Signed out');
        } finally {
            setSigningOut(false);
            setConfirmSignOut(false);
        }
    };
    return <PageToolbarProvider scope={navigation.id + ':' + (settings?.revision ?? 0)}>
        <div className="app-shell desktop-shell">
            <Toaster position="bottom-right" />
            <AppSidebar onSignOut={() => setConfirmSignOut(true)} signingOut={signingOut} />
            <section className="desktop-main">
                <EnvironmentToolbar settings={settings} />
                <main className="desktop-content dashboard-main" id="main-content" tabIndex={-1}>
                    <div className="desktop-page-heading">
                        {canGoBack && <button type="button" className="desktop-back" onClick={goBack} aria-label="Back"><ArrowLeft /></button>}
                        <div><h1>{pageTitle}</h1><p>{pageDescriptions[activeTab]}</p>
                            {targetName && <p className="desktop-target" title={targetName}>{target?.kind}: {targetName}</p>}
                        </div>
                        <PageHeadingActions />
                    </div>
                    {children}
                </main>
            </section>
            <SignOutConfirmDialog open={confirmSignOut} isSubmitting={signingOut}
                title="Sign out of RocketMQ-Rust Dashboard?"
                description="This ends the local dashboard session on this workstation."
                onCancel={() => { if (!signingOut) setConfirmSignOut(false); }}
                onConfirm={() => void handleSignOut()} />
        </div>
    </PageToolbarProvider>;
}
