import React from 'react';
import { Input as ControlInput } from './input';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
    label?: string;
    error?: string;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
    ({ label, error, id, 'aria-describedby': describedBy, 'aria-invalid': invalid, ...props }, ref) => {
        const generatedId = React.useId();
        const inputId = id ?? generatedId;
        const errorId = `${inputId}-error`;
        const description = [describedBy, error ? errorId : undefined].filter(Boolean).join(' ') || undefined;
        return (
            <div className="ops-field">
                {label && <label htmlFor={inputId} className="ops-field-label">{label}</label>}
                <ControlInput {...props} id={inputId} ref={ref}
                    aria-invalid={error ? true : invalid} aria-describedby={description} />
                {error && <p id={errorId} className="ops-field-error" role="alert">{error}</p>}
            </div>
        );
    },
);
Input.displayName = 'LegacyInput';
