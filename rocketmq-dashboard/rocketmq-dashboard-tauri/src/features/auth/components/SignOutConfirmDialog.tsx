import React from 'react';
import { LogOut } from 'lucide-react';
import { Button } from '../../../components/ui/button';
import { PageState } from '../../../components/layout/PageState';
import {
    AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription,
    AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '../../../components/ui/alert-dialog';

interface SignOutConfirmDialogProps {
    open: boolean;
    title: string;
    description: string;
    isSubmitting?: boolean;
    error?: string;
    confirmLabel?: string;
    onConfirm: () => void | Promise<void>;
    onCancel: () => void;
}

export const SignOutConfirmDialog = ({
    open, title, description, isSubmitting = false, error, confirmLabel = 'Sign Out', onConfirm, onCancel,
}: SignOutConfirmDialogProps) => (
    <AlertDialog open={open} onOpenChange={nextOpen => { if (!nextOpen && !isSubmitting) onCancel(); }}>
        <AlertDialogContent aria-busy={isSubmitting}
            onOpenAutoFocus={event => {
                // Radix normally focuses Cancel, which is disabled while a request is pending.
                if (isSubmitting && event.target instanceof HTMLElement) {
                    event.preventDefault();
                    event.target.focus();
                }
            }}
            onEscapeKeyDown={event => { if (isSubmitting) event.preventDefault(); }}>
            <AlertDialogHeader>
                <AlertDialogTitle>{title}</AlertDialogTitle>
                <AlertDialogDescription>{description}</AlertDialogDescription>
            </AlertDialogHeader>
            {error && <PageState kind="error" title="Sign out was not confirmed" description={error} />}
            <AlertDialogFooter>
                <AlertDialogCancel disabled={isSubmitting}>Cancel</AlertDialogCancel>
                <Button variant="destructive" disabled={isSubmitting} aria-busy={isSubmitting}
                    onClick={() => { if (!isSubmitting) void onConfirm(); }}>
                    <LogOut aria-hidden="true" />
                    {isSubmitting ? 'Signing Out...' : confirmLabel}
                </Button>
            </AlertDialogFooter>
        </AlertDialogContent>
    </AlertDialog>
);
