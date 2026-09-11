import * as React from 'react';
import { Slot } from '@radix-ui/react-slot@1.1.2';
import { cva, type VariantProps } from 'class-variance-authority@0.7.1';
import { cn } from './utils';

const buttonVariants = cva('ops-button', {
    variants: {
        variant: {
            default: 'ops-button-primary',
            destructive: 'ops-button-danger',
            outline: 'ops-button-outline',
            secondary: 'ops-button-secondary',
            ghost: 'ops-button-ghost',
            link: 'ops-button-link',
        },
        size: {
            default: 'ops-button-default',
            sm: 'ops-button-sm',
            lg: 'ops-button-lg',
            icon: 'ops-button-icon-only',
        },
    },
    defaultVariants: { variant: 'default', size: 'default' },
});

type ButtonProps = React.ComponentPropsWithoutRef<'button'> &
    VariantProps<typeof buttonVariants> & { asChild?: boolean };

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
    ({ className, variant, size, asChild = false, type, ...props }, ref) => {
        const Comp = asChild ? Slot : 'button';
        return <Comp {...props} ref={ref} data-slot="button"
            type={asChild ? type : type ?? 'button'}
            className={cn(buttonVariants({ variant, size }), className)} />;
    },
);
Button.displayName = 'Button';

export { Button, buttonVariants };
