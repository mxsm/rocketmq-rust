import React from 'react';
import { LogOut } from 'lucide-react';
import { Button } from '../../../components/ui/button';
import {
    AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription,
    AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '../../../components/ui/alert-dialog';

interface SignOutConfirmDialogProps {
    open: boolean;
    title: string;
    description: string;
    isSubmitting?: boolean;
    onConfirm: () => void | Promise<void>;
    onCancel: () => void;
}

export const SignOutConfirmDialog = ({
    open, title, description, isSubmitting = false, onConfirm, onCancel,
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
            <AlertDialogFooter>
                <AlertDialogCancel disabled={isSubmitting}>Cancel</AlertDialogCancel>
                <Button variant="destructive" disabled={isSubmitting} aria-busy={isSubmitting}
                    onClick={() => { if (!isSubmitting) void onConfirm(); }}>
                    <LogOut aria-hidden="true" />
                    {isSubmitting ? 'Signing Out...' : 'Sign Out'}
                </Button>
            </AlertDialogFooter>
        </AlertDialogContent>
    </AlertDialog>
);
