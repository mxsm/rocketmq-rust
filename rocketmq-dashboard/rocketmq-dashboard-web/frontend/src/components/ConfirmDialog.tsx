import { AlertTriangle } from 'lucide-react';
import type { ReactNode } from 'react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle,
  AlertDialogTrigger
} from './ui/AlertDialog';

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
    <AlertDialog>
      <AlertDialogTrigger asChild>{children}</AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogTitle className="dialog-title">
          <AlertTriangle size={18} aria-hidden="true" />
          {title}
        </AlertDialogTitle>
        <AlertDialogDescription>{description}</AlertDialogDescription>
        <div className="dialog-actions">
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction onClick={onConfirm}>{confirmLabel}</AlertDialogAction>
        </div>
      </AlertDialogContent>
    </AlertDialog>
  );
}
