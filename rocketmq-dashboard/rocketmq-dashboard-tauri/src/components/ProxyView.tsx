import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { CheckCircle2, Activity, Network, Trash2, Server, Plus } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Card } from '../src/components/ui/LegacyCard';
import { Button } from '../src/components/ui/LegacyButton';
import { Input } from '../src/components/ui/LegacyInput';

export const ProxyView = () => {
  const [proxies, setProxies] = useState(['127.0.0.1:8080', '192.168.1.50:8080']);
  const [newProxy, setNewProxy] = useState('');

  const handleAddProxy = () => {
    if (!newProxy) return toast.error('Please enter a proxy address');
    if (proxies.includes(newProxy)) return toast.error('This proxy already exists');
    setProxies([...proxies, newProxy]);
    setNewProxy('');
    toast.success('Proxy server added successfully');
  };

  const removeProxy = (proxy: string) => {
    setProxies(proxies.filter(p => p !== proxy));
    toast.success('Proxy server removed');
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
          <div className="w-10 h-10 rounded-full bg-green-50 dark:bg-green-900/30 flex items-center justify-center">
            <CheckCircle2 className="w-5 h-5 text-green-600 dark:text-green-400" />
          </div>
          <div>
            <div className="text-sm text-gray-500 dark:text-gray-400">Active Proxies</div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">{proxies.length}</div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
          <div className="w-10 h-10 rounded-full bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center">
            <Activity className="w-5 h-5 text-blue-600 dark:text-blue-400" />
          </div>
          <div>
            <div className="text-sm text-gray-500 dark:text-gray-400">Status</div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">Running</div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
          <div className="w-10 h-10 rounded-full bg-purple-50 dark:bg-purple-900/30 flex items-center justify-center">
            <Network className="w-5 h-5 text-purple-600 dark:text-purple-400" />
          </div>
          <div>
            <div className="text-sm text-gray-500 dark:text-gray-400">Traffic Mode</div>
            <div className="text-xl font-bold text-gray-900 dark:text-white">Direct</div>
          </div>
        </div>
      </div>

      <Card title="Proxy Server Address List" description="Manage the list of proxy servers connected to the dashboard.">
        <div className="space-y-8">
          
          <div className="bg-gray-50/50 dark:bg-gray-900/50 rounded-xl p-5 border border-gray-100 dark:border-gray-800">
             <label className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-3 block">Add New Proxy</label>
             <div className="flex gap-3">
               <div className="flex-1">
                 <Input 
                   placeholder="Enter proxy address (e.g. 192.168.1.50:8080)" 
                   value={newProxy} 
                   onChange={(e) => setNewProxy(e.target.value)} 
                   className="bg-white dark:bg-gray-800 dark:text-white dark:border-gray-700 shadow-sm"
                 />
               </div>
               <Button 
                 onClick={handleAddProxy} 
                 icon={Plus}
                 className="dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
               >
                 Add Server
               </Button>
             </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-4">
               <h4 className="text-sm font-semibold text-gray-900 dark:text-white">Connected Servers</h4>
               <span className="text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded-full">{proxies.length} total</span>
            </div>
            
            <div className="space-y-3">
              <AnimatePresence mode="popLayout">
                {proxies.map((proxy) => (
                  <motion.div
                    layout
                    key={proxy}
                    initial={{ opacity: 0, y: 10, scale: 0.98 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.95, transition: { duration: 0.2 } }}
                    transition={{ type: "spring", stiffness: 500, damping: 30 }}
                    className="group flex items-center justify-between p-4 bg-white dark:bg-gray-900 border border-gray-100 dark:border-gray-800 rounded-xl hover:border-blue-200 dark:hover:border-blue-800 hover:shadow-md transition-all duration-200"
                  >
                    <div className="flex items-center space-x-4">
                      <div className="w-8 h-8 rounded-lg bg-gray-100 dark:bg-gray-800 flex items-center justify-center group-hover:bg-blue-50 dark:group-hover:bg-blue-900/30 transition-colors">
                        <Server className="w-4 h-4 text-gray-500 dark:text-gray-400 group-hover:text-blue-600 dark:group-hover:text-blue-400" />
                      </div>
                      <div>
                        <div className="text-sm font-medium text-gray-900 dark:text-white font-mono">{proxy}</div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 flex items-center mt-0.5">
                          <span className="w-1.5 h-1.5 rounded-full bg-green-500 mr-1.5"></span>
                          Online
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-center space-x-2 opacity-0 group-hover:opacity-100 transition-opacity">
                      <Button variant="ghost" className="text-red-500 dark:text-red-400 hover:text-red-600 dark:hover:text-red-300 hover:bg-red-50 dark:hover:bg-red-900/20" onClick={() => removeProxy(proxy)}>
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
          </div>

        </div>
      </Card>
    </div>
  );
};
