import * as AlertDialog from '@radix-ui/react-alert-dialog';
import { AlertTriangle } from 'lucide-react';
import type { ReactNode } from 'react';

interface ConfirmDialogProps {
  title: string;
  description: string;
  confirmLabel?: string;
  children: ReactNode;
  onConfirm: () => void;
}

export default function ConfirmDialog({
  title,
  description,
  confirmLabel = 'Confirm',
  children,
  onConfirm
}: ConfirmDialogProps) {
  return (
    <AlertDialog.Root>
      <AlertDialog.Trigger asChild>{children}</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="dialog-overlay" />
        <AlertDialog.Content className="dialog-content">
          <AlertDialog.Title className="dialog-title">
            <AlertTriangle size={18} aria-hidden="true" />
            {title}
          </AlertDialog.Title>
          <AlertDialog.Description className="dialog-description">{description}</AlertDialog.Description>
          <div className="dialog-actions">
            <AlertDialog.Cancel className="button button-secondary">Cancel</AlertDialog.Cancel>
            <AlertDialog.Action className="button button-danger" onClick={onConfirm}>
              {confirmLabel}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}
