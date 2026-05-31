import React from 'react';
import {motion} from 'motion/react';

interface ButtonProps {
    children: React.ReactNode;
    variant?: 'primary' | 'secondary' | 'ghost' | 'accent' | 'danger' | 'outline';
    onClick?: () => void;
    icon?: React.ElementType;
    iconClassName?: string;
    className?: string;
    disabled?: boolean;
}

export const Button = ({
    children,
    variant = 'primary',
    onClick,
    icon: Icon,
    iconClassName = "",
    className = "",
    disabled
}: ButtonProps) => {
    const variants = {
        primary: "ops-button-primary",
        secondary: "ops-button-secondary",
        ghost: "ops-button-ghost",
        accent: "ops-button-accent",
        danger: "ops-button-danger",
        outline: "ops-button-outline"
    };

    return (
        <motion.button
            whileTap={{scale: 0.98}}
            onClick={onClick}
            disabled={disabled}
            className={`ops-button ${variants[variant]} ${className}`}
        >
            {Icon && <Icon className={`ops-button-icon ${iconClassName}`}/>}
            {children}
        </motion.button>
    );
};
