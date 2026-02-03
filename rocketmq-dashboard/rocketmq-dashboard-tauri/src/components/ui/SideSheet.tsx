import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { X } from 'lucide-react';

interface SideSheetProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  data: Record<string, any>;
}

export const SideSheet = ({ isOpen, onClose, title, data }: SideSheetProps) => {
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
