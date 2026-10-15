import { Skeleton } from './ui/Skeleton';
import './RouteLoading.css';

interface RouteLoadingProps {
  /** Fill the viewport, for routes that load outside the application shell. */
  fullscreen?: boolean;
}

/** Skeleton shaped like a workspace page, so content does not jump when the route module arrives. */
export default function RouteLoading({ fullscreen = false }: RouteLoadingProps) {
  return (
    <div
      className={fullscreen ? 'route-loading route-loading-fullscreen' : 'route-loading'}
      role="status"
      aria-live="polite"
      aria-label="Loading dashboard workspace"
    >
      <div className="route-loading-heading">
        <Skeleton />
        <Skeleton />
      </div>
      <div className="route-loading-metrics">
        <Skeleton />
        <Skeleton />
        <Skeleton />
        <Skeleton />
      </div>
      <Skeleton className="route-loading-panel" />
    </div>
  );
}
