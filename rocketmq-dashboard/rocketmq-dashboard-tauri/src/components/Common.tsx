import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { X } from 'lucide-react';

// --- Card ---
export const Card = ({ children, className = "", title, description, headerAction }: any) => (
  <div className={`ops-card ${className}`}>
    {(title || description) && (
      <div className="ops-card-header">
        <div>
          {title && <h3 className="ops-card-title">{title}</h3>}
          {description && <p className="ops-card-description">{description}</p>}
        </div>
        {headerAction}
      </div>
    )}
    {children}
  </div>
);

// --- Toggle ---
export const Toggle = ({ label, checked, onChange }: any) => (
  <div className="ops-toggle-row">
    <span>{label}</span>
    <button
      onClick={() => onChange(!checked)}
      className={`ops-switch ${checked ? 'is-on' : ''}`}
    >
      <span className="ops-switch-thumb" />
    </button>
  </div>
);

// --- Button ---
export const Button = ({ children, variant = 'primary', onClick, icon: Icon, className = "" }: any) => {
  const variants: Record<string, string> = {
    primary: "ops-button-primary",
    secondary: "ops-button-secondary",
    ghost: "ops-button-ghost",
    accent: "ops-button-accent",
    danger: "ops-button-danger",
    outline: "ops-button-outline"
  };

  return (
    <motion.button
      whileTap={{ scale: 0.98 }}
      onClick={onClick}
      className={`ops-button ${variants[variant]} ${className}`}
    >
      {Icon && <Icon className="ops-button-icon" />}
      {children}
    </motion.button>
  );
};

// --- Input ---
export const Input = ({ placeholder, value, onChange, className = "" }: any) => (
  <input
    type="text"
    value={value}
    onChange={onChange}
    placeholder={placeholder}
    className={`ops-input ${className}`}
  />
);

// --- SideSheet ---
export const SideSheet = ({ isOpen, onClose, title, data }: any) => {
  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.42 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="ops-sheet-backdrop"
          />
          <motion.div
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: "spring", stiffness: 300, damping: 30 }}
            className="ops-sheet"
          >
            <div className="ops-sheet-header">
              <h2 className="ops-sheet-title">{title}</h2>
              <button onClick={onClose} className="ops-icon-button" aria-label="Close details">
                <X className="w-5 h-5" />
              </button>
            </div>
            
            <div className="ops-sheet-content">
               <table className="ops-table">
                  <thead>
                    <tr>
                      <th className="w-1/3">Key</th>
                      <th>Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(data).map(([key, value]) => (
                      <tr key={key}>
                        <td className="ops-table-key">{key}</td>
                        <td className="ops-table-value">{String(value)}</td>
                      </tr>
                    ))}
                  </tbody>
               </table>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
};
