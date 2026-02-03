import React from 'react';
import { Card } from '../../../components/ui/LegacyCard';
import { Activity, Calendar } from 'lucide-react';

export const BrokerOverview = () => {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Broker Overview */}
      <div className="lg:col-span-2">
        <Card title="Broker Overview" className="h-full">
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
              <thead className="text-xs text-gray-500 uppercase bg-gray-50/50 border-b border-gray-100 dark:border-gray-800">
                <tr>
                  <th className="px-4 py-3 font-medium bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Broker Name</th>
                  <th className="px-4 py-3 font-medium bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Address</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Today Received</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Today Produced</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Yesterday</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                <tr className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                  <td className="px-4 py-3 font-medium text-gray-900 dark:text-gray-100">mixin0</td>
                  <td className="px-4 py-3 font-mono text-gray-600 dark:text-gray-400">172.20.48.1:10911</td>
                  <td className="px-4 py-3 text-right font-medium text-blue-600 dark:text-blue-400">14,205</td>
                  <td className="px-4 py-3 text-right dark:text-gray-300">14,205</td>
                  <td className="px-4 py-3 text-right text-gray-500 dark:text-gray-500">12,100</td>
                </tr>
                <tr className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                  <td className="px-4 py-3 font-medium text-gray-900 dark:text-gray-100">mixin1</td>
                  <td className="px-4 py-3 font-mono text-gray-600 dark:text-gray-400">172.20.48.2:10911</td>
                  <td className="px-4 py-3 text-right font-medium text-blue-600 dark:text-blue-400">8,450</td>
                  <td className="px-4 py-3 text-right dark:text-gray-300">8,450</td>
                  <td className="px-4 py-3 text-right text-gray-500 dark:text-gray-500">9,320</td>
                </tr>
              </tbody>
            </table>
          </div>
        </Card>
      </div>

      {/* Date / Quick Stats */}
      <div className="space-y-6">
         <div className="bg-white dark:bg-gray-900 p-6 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm flex items-center justify-between">
            <div>
              <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Current Date</h3>
              <p className="text-xl font-bold text-gray-900 dark:text-white mt-1">2026-01-27</p>
            </div>
            <div className="w-10 h-10 rounded-xl bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400">
              <Calendar className="w-5 h-5" />
            </div>
         </div>
         
         <div className="bg-white dark:bg-gray-900 rounded-2xl p-6 border border-gray-100 dark:border-gray-800 shadow-sm">
            <div className="flex items-center space-x-2 mb-4 text-gray-500 dark:text-gray-400">
                <Activity className="w-4 h-4 text-blue-500" />
                <span className="text-sm font-medium">Total Throughput</span>
            </div>
            <div className="text-3xl font-bold tracking-tight text-gray-900 dark:text-white">24.5k</div>
            <div className="text-sm text-gray-500 dark:text-gray-400 mt-1">Messages / sec</div>
         </div>
      </div>
    </div>
  );
};
