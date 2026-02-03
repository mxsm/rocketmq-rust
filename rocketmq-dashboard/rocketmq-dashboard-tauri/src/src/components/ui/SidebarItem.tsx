import React from 'react';
import { motion } from 'motion/react';

interface SidebarItemProps {
  icon: React.ElementType;
  label: string;
  active: boolean;
  onClick: () => void;
}

export const SidebarItem = ({ icon: Icon, label, active, onClick }: SidebarItemProps) => (
  <button
    onClick={onClick}
    className={`w-full flex items-center px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 group mb-1 ${
      active 
        ? 'bg-blue-50/80 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400' 
        : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-200'
    }`}
  >
    <Icon className={`w-5 h-5 mr-3 transition-colors ${active ? 'text-blue-500 dark:text-blue-400' : 'text-gray-400 group-hover:text-gray-600 dark:text-gray-500 dark:group-hover:text-gray-300'}`} />
    {label}
    {active && (
      <motion.div
        layoutId="active-indicator"
        className="ml-auto w-1.5 h-1.5 rounded-full bg-blue-500 dark:bg-blue-400"
      />
    )}
  </button>
);
