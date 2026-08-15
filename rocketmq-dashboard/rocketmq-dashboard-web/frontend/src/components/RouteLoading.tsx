import './RouteLoading.css';

export default function RouteLoading() {
  return (
    <div className="route-loading" role="status" aria-live="polite" aria-label="Loading dashboard workspace">
      <p>Loading dashboard workspace</p>
    </div>
  );
}
