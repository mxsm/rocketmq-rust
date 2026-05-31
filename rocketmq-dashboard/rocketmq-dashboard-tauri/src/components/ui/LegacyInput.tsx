import React from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className = "", label, error, ...props }, ref) => {
    return (
      <div className="ops-field">
        {label && (
          <label className="ops-field-label">
            {label}
          </label>
        )}
        <div className="relative">
          <input
            ref={ref}
            className={`ops-input ${error ? 'is-invalid' : ''} ${className}`}
            {...props}
          />
        </div>
        {error && <p className="ops-field-error" role="alert">{error}</p>}
      </div>
    );
  }
);
Input.displayName = 'Input';
