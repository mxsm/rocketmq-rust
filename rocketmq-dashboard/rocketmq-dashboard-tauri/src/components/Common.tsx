import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { X } from 'lucide-react';

// --- Card ---
export const Card = ({ children, className = "", title, description, headerAction }: any) => (
  <div className={`bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm p-6 transition-colors duration-200 ${className}`}>
    {(title || description) && (
      <div className="mb-6 flex justify-between items-start">
        <div>
          {title && <h3 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h3>}
          {description && <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">{description}</p>}
        </div>
        {headerAction}
      </div>
    )}
    {children}
  </div>
);

// --- Toggle ---
export const Toggle = ({ label, checked, onChange }: any) => (
  <div className="flex items-center justify-between py-4">
    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">{label}</span>
    <button
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-7 w-12 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-900 ${
        checked ? 'bg-blue-500' : 'bg-gray-200 dark:bg-gray-700'
      }`}
    >
      <span
        className={`inline-block h-5 w-5 transform rounded-full bg-white transition-transform shadow-sm ${
          checked ? 'translate-x-6' : 'translate-x-1'
        }`}
      />
    </button>
  </div>
);

// --- Button ---
export const Button = ({ children, variant = 'primary', onClick, icon: Icon, className = "" }: any) => {
  const baseStyle = "inline-flex items-center justify-center px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1 dark:focus:ring-offset-gray-900 disabled:opacity-50 disabled:cursor-not-allowed";
  
  const variants: Record<string, string> = {
    primary: "bg-gray-900 text-white hover:bg-gray-800 focus:ring-gray-900 dark:bg-white dark:text-gray-900 dark:hover:bg-gray-200 dark:focus:ring-white shadow-sm hover:shadow",
    secondary: "bg-white text-gray-700 border border-gray-200 hover:bg-gray-50 focus:ring-gray-200 dark:bg-gray-900 dark:text-gray-300 dark:border-gray-700 dark:hover:bg-gray-800",
    ghost: "bg-transparent text-gray-600 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white",
    accent: "bg-blue-500 text-white hover:bg-blue-600 focus:ring-blue-500 shadow-sm hover:shadow-md",
    danger: "bg-white text-red-600 border border-red-100 hover:bg-red-50 focus:ring-red-200 dark:bg-gray-900 dark:border-red-900/50 dark:hover:bg-red-900/20",
    outline: "bg-transparent text-gray-700 border border-gray-300 hover:bg-gray-50 focus:ring-gray-200 dark:text-gray-300 dark:border-gray-700 dark:hover:bg-gray-800"
  };

  return (
    <motion.button
      whileTap={{ scale: 0.98 }}
      onClick={onClick}
      className={`${baseStyle} ${variants[variant]} ${className}`}
    >
      {Icon && <Icon className="w-4 h-4 mr-2" />}
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
    className={`w-full px-4 py-2.5 bg-gray-50 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-gray-400 dark:bg-gray-800 dark:border-gray-700 dark:text-white dark:placeholder:text-gray-500 ${className}`}
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
            animate={{ opacity: 0.3 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 bg-black z-40"
          />
          <motion.div
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: "spring", stiffness: 300, damping: 30 }}
            className="fixed right-0 top-0 bottom-0 w-full max-w-2xl bg-white dark:bg-gray-900 shadow-2xl z-50 flex flex-col border-l border-gray-200 dark:border-gray-800"
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-100 dark:border-gray-800">
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">{title}</h2>
              <button onClick={onClose} className="p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors">
                <X className="w-5 h-5 text-gray-500 dark:text-gray-400" />
              </button>
            </div>
            
            <div className="flex-1 overflow-y-auto p-0">
               <table className="w-full text-sm text-left">
                  <thead className="text-xs text-gray-500 dark:text-gray-400 uppercase bg-gray-50 dark:bg-gray-800/50 sticky top-0">
                    <tr>
                      <th className="px-6 py-3 font-medium w-1/3">Key</th>
                      <th className="px-6 py-3 font-medium">Value</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {Object.entries(data).map(([key, value]) => (
                      <tr key={key} className="hover:bg-gray-50/50 dark:hover:bg-gray-800/50">
                        <td className="px-6 py-3 font-medium text-gray-700 dark:text-gray-300 break-words">{key}</td>
                        <td className="px-6 py-3 text-gray-600 dark:text-gray-400 font-mono text-xs break-all">{String(value)}</td>
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
