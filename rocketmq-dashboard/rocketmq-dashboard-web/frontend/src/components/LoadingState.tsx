import { Loader2 } from 'lucide-react';

interface LoadingStateProps {
  label?: string;
}

export default function LoadingState({ label = 'Loading data' }: LoadingStateProps) {
  return (
    <div className="state-block">
      <Loader2 className="spin" size={24} aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}
