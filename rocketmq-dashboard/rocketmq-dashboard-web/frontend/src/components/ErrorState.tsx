import { AlertTriangle } from 'lucide-react';
import { Button } from './ui/Button';

interface ErrorStateProps {
  message: string;
  onRetry?: () => void;
}

export default function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="state-block state-block-error" role="alert">
      <AlertTriangle size={24} aria-hidden="true" />
      <span>{message}</span>
      {onRetry ? (
        <Button type="button" variant="secondary" onClick={onRetry}>
          Retry
        </Button>
      ) : null}
    </div>
  );
}
