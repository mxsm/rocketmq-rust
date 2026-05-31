import React from 'react';
import { motion } from 'motion/react';

interface ToggleProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
  disabled?: boolean;
  className?: string;
}

export const Toggle = ({ checked, onChange, label, disabled = false, className = "" }: ToggleProps) => {
  return (
    <div className={`ops-switch-field ${className}`}>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        onClick={() => !disabled && onChange(!checked)}
        className={`ops-switch ${checked ? 'is-on' : ''} ${disabled ? 'is-disabled' : ''}`}
      >
        <span className="sr-only">Use setting</span>
        <span className="ops-switch-thumb" />
      </button>
      {label && (
        <span 
          className="ops-switch-label"
          onClick={() => !disabled && onChange(!checked)}
        >
          {label}
        </span>
      )}
    </div>
  );
};
