import * as React from 'react';
import { cn } from './utils';

const Input = React.forwardRef<HTMLInputElement, React.ComponentPropsWithoutRef<'input'>>(
    ({ className, ...props }, ref) => (
        <input {...props} ref={ref} data-slot="input" className={cn('ops-input', className)} />
    ),
);
Input.displayName = 'Input';

export { Input };
