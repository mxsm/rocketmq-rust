import React from 'react';

type ToggleProps = Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'onChange' | 'onClick' | 'children'> & {
    checked: boolean;
    onChange: (checked: boolean) => void;
    label?: string;
};

export const Toggle = React.forwardRef<HTMLButtonElement, ToggleProps>(
    ({ checked, onChange, label, disabled = false, className = '', id, ...props }, ref) => {
        const generatedId = React.useId();
        const controlId = id ?? generatedId;
        return (
            <div className={`ops-switch-field ${className}`}>
                <button {...props} ref={ref} id={controlId} type="button" role="switch"
                    aria-checked={checked} disabled={disabled}
                    onClick={() => onChange(!checked)}
                    className={`ops-switch ${checked ? 'is-on' : ''}`}>
                    <span className="ops-switch-thumb" />
                </button>
                {label && <label htmlFor={controlId} className="ops-switch-label">{label}</label>}
            </div>
        );
    },
);
Toggle.displayName = 'LegacyToggle';
