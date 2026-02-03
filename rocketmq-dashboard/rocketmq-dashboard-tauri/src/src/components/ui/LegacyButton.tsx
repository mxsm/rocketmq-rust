import React from 'react';
import { motion } from 'motion/react';

interface ButtonProps {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary' | 'ghost' | 'accent' | 'danger' | 'outline';
  onClick?: () => void;
  icon?: React.ElementType;
  className?: string;
  disabled?: boolean;
}

export const Button = ({ children, variant = 'primary', onClick, icon: Icon, className = "", disabled }: ButtonProps) => {
  const baseStyle = "inline-flex items-center justify-center px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1 dark:focus:ring-offset-gray-900 disabled:opacity-50 disabled:cursor-not-allowed";
  
  const variants = {
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
      disabled={disabled}
      className={`${baseStyle} ${variants[variant]} ${className}`}
    >
      {Icon && <Icon className="w-4 h-4 mr-2" />}
      {children}
    </motion.button>
  );
};
