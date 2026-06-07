import { Inbox } from 'lucide-react';

interface EmptyStateProps {
  title?: string;
  detail?: string;
}

export default function EmptyState({ title = 'No data', detail = 'Nothing matched the current query.' }: EmptyStateProps) {
  return (
    <div className="state-block">
      <Inbox size={24} aria-hidden="true" />
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}
