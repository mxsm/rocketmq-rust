import React from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className = "", label, error, ...props }, ref) => {
    return (
      <div className="w-full">
        {label && (
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1.5">
            {label}
          </label>
        )}
        <div className="relative">
          <input
            ref={ref}
            className={`
              block w-full px-4 py-2.5 
              bg-white dark:bg-gray-900 
              border border-gray-200 dark:border-gray-700 
              rounded-xl 
              text-gray-900 dark:text-white 
              placeholder-gray-500 dark:placeholder-gray-400
              focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 
              disabled:opacity-50 disabled:cursor-not-allowed
              transition-all duration-200
              ${error ? 'border-red-500 focus:border-red-500 focus:ring-red-500/20' : ''}
              ${className}
            `}
            {...props}
          />
        </div>
        {error && <p className="mt-1 text-sm text-red-500">{error}</p>}
      </div>
    );
  }
);
Input.displayName = 'Input';
