import { Skeleton } from './ui/Skeleton';

interface LoadingStateProps {
  label?: string;
}

export default function LoadingState({ label = 'Loading data' }: LoadingStateProps) {
  return (
    <div className="state-block" role="status" aria-label={label}>
      <div className="state-skeleton" aria-hidden="true">
        <Skeleton />
        <Skeleton />
        <Skeleton />
      </div>
      <span>{label}</span>
    </div>
  );
}
