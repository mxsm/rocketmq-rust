import { AlertTriangle } from 'lucide-react';

interface ErrorStateProps {
  message: string;
  onRetry?: () => void;
}

export default function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="state-block state-block-error">
      <AlertTriangle size={24} aria-hidden="true" />
      <span>{message}</span>
      {onRetry ? (
        <button type="button" className="button button-secondary" onClick={onRetry}>
          Retry
        </button>
      ) : null}
    </div>
  );
}
