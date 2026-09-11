import React from 'react';
import { Button as ControlButton } from './button';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: 'primary' | 'secondary' | 'ghost' | 'accent' | 'danger' | 'outline';
    icon?: React.ElementType;
    iconClassName?: string;
}

const variants = {
    primary: 'default',
    secondary: 'secondary',
    ghost: 'ghost',
    accent: 'default',
    danger: 'destructive',
    outline: 'outline',
} as const;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
    ({ children, variant = 'primary', icon: Icon, iconClassName = '', ...props }, ref) => (
        <ControlButton {...props} ref={ref} variant={variants[variant]}>
            {Icon && <Icon aria-hidden="true" className={`ops-button-icon ${iconClassName}`} />}
            {children}
        </ControlButton>
    ),
);
Button.displayName = 'LegacyButton';
