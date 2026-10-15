import type { HTMLAttributes } from 'react';
import { cn } from '../../lib/cn';

export type StatusDotTone = 'success' | 'warning' | 'danger' | 'info' | 'primary' | 'neutral';

interface StatusDotProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: StatusDotTone;
}

/**
 * A pulsing status light. It is decorative: the state it shows must also be present
 * as text or an icon next to it, so the dot is hidden from assistive technology.
 */
export function StatusDot({ tone = 'success', className, ...props }: StatusDotProps) {
  return <span className={cn('status-dot', className)} data-tone={tone} aria-hidden="true" {...props} />;
}
