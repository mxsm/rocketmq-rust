import { ArrowLeft } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { cn } from '../lib/cn';
import PageHeader from './PageHeader';
import { buttonVariants } from './ui/Button';

interface EntityDetailPageProps {
  title: string;
  description: string;
  backTo: string;
  backLabel: string;
  actions?: ReactNode;
  className?: string;
  children: ReactNode;
}

export default function EntityDetailPage({
  title,
  description,
  backTo,
  backLabel,
  actions,
  className,
  children
}: EntityDetailPageProps) {
  return (
    <div className={cn('entity-full-page', className)} data-surface="frosted">
      <div className="entity-full-page-header">
        <PageHeader
          title={title}
          description={description}
          actions={
            <>
              <Link className={buttonVariants({ variant: 'outline', size: 'sm' })} to={backTo}>
                <ArrowLeft size={14} aria-hidden="true" /> {backLabel}
              </Link>
              {actions}
            </>
          }
        />
      </div>
      <div className="entity-full-page-body">{children}</div>
    </div>
  );
}
