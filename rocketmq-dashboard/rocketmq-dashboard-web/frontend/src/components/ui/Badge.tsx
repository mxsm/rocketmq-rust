import { cva, type VariantProps } from 'class-variance-authority';
import type { HTMLAttributes } from 'react';
import { cn } from '../../lib/cn';

const badgeVariants = cva('ui-badge', {
  variants: {
    tone: {
      neutral: 'ui-badge-neutral',
      success: 'ui-badge-success',
      warning: 'ui-badge-warning',
      destructive: 'ui-badge-destructive',
      info: 'ui-badge-info'
    }
  },
  defaultVariants: { tone: 'neutral' }
});

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, tone, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ tone }), className)} data-tone={tone ?? 'neutral'} {...props} />;
}
