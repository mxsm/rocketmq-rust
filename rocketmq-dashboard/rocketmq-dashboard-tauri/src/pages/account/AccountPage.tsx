import React, { useEffect, useState } from 'react';
import {
    CalendarDays,
    KeyRound,
    LogOut,
    RefreshCw,
    ShieldCheck,
    ShieldAlert,
    UserRound,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { AuthService } from '../../services/auth.service';
import { DashboardClientError, dashboardErrorMessage } from '../../services/invoke';
import { SessionStorageService } from '../../services/session.storage';
import { useAppStore } from '../../stores/app.store';
import { SignOutConfirmDialog, useAuth } from '../../features/auth';
import { ChangePasswordDialog } from '../../features/auth/components/ChangePasswordDialog';
import type { UserProfile } from '../../features/auth/types/auth.types';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from '../../components/ui/card';

const glassCardClass =
    'ops-card account-card';
const secondaryActionButtonClass =
    'ops-button ops-button-outline account-action-button';
const dangerActionButtonClass =
    'ops-button ops-button-danger account-action-button';

const formatTimestamp = (value: string | null | undefined) => {
    if (!value) {
        return 'Never';
    }

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }

    return new Intl.DateTimeFormat('en-US', {
        dateStyle: 'medium',
        timeStyle: 'short',
    }).format(parsed);
};

const DetailRow = ({
    label,
    value,
    mono = false,
}: {
    label: string;
    value: string;
    mono?: boolean;
}) => (
    <div className="flex items-start justify-between gap-4 rounded-2xl border border-black/5 bg-black/[0.02] px-4 py-3 dark:border-white/6 dark:bg-white/[0.025]">
        <span className="text-sm text-gray-500 dark:text-gray-400">{label}</span>
        <span className={`text-right text-sm text-gray-900 dark:text-white ${mono ? 'font-mono text-xs sm:text-sm' : ''}`}>
            {value}
        </span>
    </div>
);

export const AccountPage = () => {
    const { clearAuthSession, currentUser, sessionId } = useAppStore();
    const { logout } = useAuth();
    const [profile, setProfile] = useState<UserProfile | null>(null);
    const [error, setError] = useState('');
    const [isLoading, setIsLoading] = useState(true);
    const [reloadKey, setReloadKey] = useState(0);
    const [isPasswordDialogOpen, setIsPasswordDialogOpen] = useState(false);
    const [isLogoutDialogOpen, setIsLogoutDialogOpen] = useState(false);
    const [isLoggingOut, setIsLoggingOut] = useState(false);

    useEffect(() => {
        let isMounted = true;

        if (!sessionId) {
            SessionStorageService.clearSessionId();
            clearAuthSession();
            return () => {
                isMounted = false;
            };
        }

        const loadProfile = async () => {
            setIsLoading(true);
            setError('');

            try {
                const result = await AuthService.getCurrentUserProfile();
                if (!isMounted) {
                    return;
                }

                setProfile(result);
            } catch (loadError) {
                if (!isMounted) {
                    return;
                }

                setProfile(null);
                if (loadError instanceof DashboardClientError && loadError.code === 'auth.session.invalid') {
                    SessionStorageService.clearSessionId();
                    clearAuthSession();
                    return;
                }
                setError(dashboardErrorMessage(loadError, 'Failed to load user profile'));
            } finally {
                if (isMounted) {
                    setIsLoading(false);
                }
            }
        };

        void loadProfile();

        return () => {
            isMounted = false;
        };
    }, [currentUser?.mustChangePassword, reloadKey, sessionId]);

    const username = profile?.username ?? currentUser?.username ?? 'Admin';
    const initials = username.slice(0, 2).toUpperCase();
    const unresolvedValue = isLoading && !profile ? 'Loading...' : 'Unavailable';

    const handleLogout = async () => {
        setIsLogoutDialogOpen(false);
        setIsLoggingOut(true);

        try {
            await logout();
            toast.success('Logged out successfully');
        } finally {
            setIsLoggingOut(false);
        }
    };

    return (
        <>
            <div className="mx-auto max-w-6xl space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                <Card className={glassCardClass}>
                    <CardContent className="flex flex-col gap-6 px-6 py-6 !pb-12 sm:px-8 sm:py-8 lg:flex-row lg:items-start lg:justify-between">
                        <div className="flex items-center gap-4">
                            <div className="flex h-16 w-16 items-center justify-center rounded-3xl bg-gradient-to-br from-slate-900 via-slate-800 to-slate-700 text-lg font-semibold text-white shadow-[0_16px_40px_rgba(15,23,42,0.28)]">
                                {initials}
                            </div>
                            <div className="space-y-2">
                                <div className="flex flex-wrap items-center gap-2">
                                    <h2 className="text-2xl font-semibold tracking-tight text-gray-900 dark:text-white">
                                        {username}
                                    </h2>
                                    <Badge
                                        variant="outline"
                                        className="border-emerald-200/80 bg-emerald-500/10 text-emerald-700 dark:border-emerald-400/20 dark:bg-emerald-400/10 dark:text-emerald-300"
                                    >
                                        <ShieldCheck className="h-3 w-3" />
                                        {profile?.isActive ?? true ? 'Active' : 'Disabled'}
                                    </Badge>
                                    {profile?.mustChangePassword ? (
                                        <Badge
                                            variant="outline"
                                            className="border-amber-200/80 bg-amber-500/10 text-amber-700 dark:border-amber-400/20 dark:bg-amber-400/10 dark:text-amber-300"
                                        >
                                            <ShieldAlert className="h-3 w-3" />
                                            Password update required
                                        </Badge>
                                    ) : null}
                                </div>
                                <p className="max-w-2xl text-sm leading-6 text-gray-500 dark:text-gray-400">
                                    Review the local administrator account, session security posture, and account lifecycle
                                    details for this dashboard instance.
                                </p>
                            </div>
                        </div>

                        <div className="flex flex-wrap items-center pt-4 pb-8 lg:pt-0" style={{ gap: '24px', paddingBottom: '16px' }}>
                            <Button
                                type="button"
                                variant="outline"
                                onClick={() => setReloadKey((value) => value + 1)}
                                className={secondaryActionButtonClass}
                                style={{ paddingLeft: '24px', paddingRight: '24px' }}
                            >
                                <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
                                Refresh
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                onClick={() => setIsPasswordDialogOpen(true)}
                                className={secondaryActionButtonClass}
                                style={{ paddingLeft: '24px', paddingRight: '24px' }}
                            >
                                <KeyRound className="h-4 w-4" />
                                Change Password
                            </Button>
                        </div>
                    </CardContent>
                </Card>

                {error ? (
                    <Card className={`${glassCardClass} border-rose-200/70 dark:border-rose-500/20`}>
                        <CardHeader>
                            <CardTitle className="text-gray-900 dark:text-white">Profile unavailable</CardTitle>
                            <CardDescription className="text-rose-600 dark:text-rose-300">
                                {error}
                            </CardDescription>
                        </CardHeader>
                    </Card>
                ) : null}

                <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
                    <Card className={glassCardClass}>
                        <CardHeader className="pb-4">
                            <CardTitle className="flex items-center gap-2 text-gray-900 dark:text-white">
                                <UserRound className="h-4 w-4 text-sky-500" />
                                Basic details
                            </CardTitle>
                            <CardDescription>Persistent account fields stored by the local auth database.</CardDescription>
                        </CardHeader>
                        <CardContent className="!pb-8">
                            <div className="space-y-4 mb-2">
                                <DetailRow label="Username" value={profile?.username ?? username} />
                                <DetailRow label="User ID" value={profile ? String(profile.userId) : unresolvedValue} mono />
                                <DetailRow label="Created At" value={profile ? formatTimestamp(profile.createdAt) : unresolvedValue} />
                                <DetailRow label="Updated At" value={profile ? formatTimestamp(profile.updatedAt) : unresolvedValue} />
                            </div>
                        </CardContent>
                    </Card>

                    <Card className={glassCardClass}>
                        <CardHeader className="pb-4">
                            <CardTitle className="flex items-center gap-2 text-gray-900 dark:text-white">
                                <ShieldCheck className="h-4 w-4 text-sky-500" />
                                Security status
                            </CardTitle>
                            <CardDescription>Current access posture and login hygiene for this administrator.</CardDescription>
                        </CardHeader>
                        <CardContent className="!pb-8">
                            <div className="space-y-4 mb-2">
                                <DetailRow
                                    label="Account Status"
                                    value={profile ? (profile.isActive ? 'Enabled' : 'Disabled') : unresolvedValue}
                                />
                                <DetailRow
                                    label="Password Status"
                                    value={profile ? (profile.mustChangePassword ? 'Change required' : 'Up to date') : unresolvedValue}
                                />
                                <DetailRow label="Last Login" value={profile ? formatTimestamp(profile.lastLoginAt) : unresolvedValue} />
                                <div className="rounded-2xl border border-sky-200/70 bg-sky-500/[0.08] px-4 py-3 text-sm text-sky-700 dark:border-sky-400/15 dark:bg-sky-400/[0.08] dark:text-sky-200">
                                    The dashboard keeps authentication local to this workstation. Password changes apply to
                                    future sign-ins immediately.
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </div>

                <Card className={glassCardClass}>
                    <CardHeader className="pb-4">
                        <CardTitle className="flex items-center gap-2 text-gray-900 dark:text-white">
                            <CalendarDays className="h-4 w-4 text-sky-500" />
                            Session & actions
                        </CardTitle>
                        <CardDescription>Manage the authenticated local account lifecycle.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4 !pb-14">
                        <div className="rounded-2xl border border-black/5 bg-black/[0.02] px-4 py-4 dark:border-white/6 dark:bg-white/[0.025]">
                            <div className="text-sm font-medium text-gray-900 dark:text-white">Authenticated locally</div>
                            <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                Session credentials remain private and are never displayed or copied.
                            </div>
                        </div>

                        <div className="flex flex-wrap gap-6 pt-6 pb-4">
                            <Button
                                type="button"
                                variant="outline"
                                onClick={() => setIsPasswordDialogOpen(true)}
                                className={secondaryActionButtonClass}
                            >
                                <KeyRound className="h-4 w-4" />
                                Change Password
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                onClick={() => setIsLogoutDialogOpen(true)}
                                className={dangerActionButtonClass}
                            >
                                <LogOut className="h-4 w-4" />
                                Sign Out
                            </Button>
                        </div>
                    </CardContent>
                </Card>
            </div>

            <ChangePasswordDialog
                open={isPasswordDialogOpen}
                onOpenChange={setIsPasswordDialogOpen}
                onPasswordChanged={() => setReloadKey((value) => value + 1)}
            />

            <SignOutConfirmDialog
                open={isLogoutDialogOpen}
                isSubmitting={isLoggingOut}
                title="Sign out of this workstation?"
                description="Your local session will be removed from this device. You can sign back in at any time with your administrator credentials."
                onCancel={() => setIsLogoutDialogOpen(false)}
                onConfirm={() => void handleLogout()}
            />
        </>
    );
};
