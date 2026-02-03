import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { Server, Trash2, Plus } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Card } from '../src/components/ui/LegacyCard';
import { Button } from '../src/components/ui/LegacyButton';
import { Input } from '../src/components/ui/LegacyInput';
import { Toggle } from '../src/components/ui/LegacyToggle';

export const OpsView = () => {
  const [nameServers, setNameServers] = useState(['127.0.0.1:9876', '192.168.1.20:9876']);
  const [newNameServer, setNewNameServer] = useState('');
  const [selectedNameServer, setSelectedNameServer] = useState('127.0.0.1:9876');
  const [isVIPChannel, setIsVIPChannel] = useState(true);
  const [useTLS, setUseTLS] = useState(false);

  const handleAddNameServer = () => {
    if (!newNameServer) return toast.error('Please enter a valid address');
    if (nameServers.includes(newNameServer)) return toast.error('Address already exists');
    setNameServers([...nameServers, newNameServer]);
    setNewNameServer('');
    toast.success('Name Server added');
  };

  const handleRemoveNameServer = (ns: string) => {
    if (nameServers.length <= 1) return toast.error('Cannot remove the last Name Server');
    if (ns === selectedNameServer) return toast.error('Cannot remove the active Name Server');
    
    setNameServers(nameServers.filter(s => s !== ns));
    toast.success('Name Server removed');
  };

  const handleSwitchServer = (ns: string) => {
    setSelectedNameServer(ns);
    toast.success(`Switched to Name Server: ${ns}`);
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <Card title="Name Server Configuration" description="Manage your Name Server addresses and connection status.">
        <div className="space-y-6">
          
          <div className="flex gap-3 items-start">
            <div className="flex-1">
              <Input 
                placeholder="Enter new Name Server address (e.g. 192.168.1.50:9876)" 
                value={newNameServer}
                onChange={(e) => setNewNameServer(e.target.value)}
                className="bg-white dark:bg-gray-800 dark:text-white dark:border-gray-700 shadow-sm"
              />
            </div>
            <Button 
              variant="primary" 
              onClick={handleAddNameServer} 
              icon={Plus}
              className="dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
            >
              Add Server
            </Button>
          </div>

          <div className="border border-gray-200 rounded-xl overflow-hidden bg-white shadow-sm">
            <div className="min-w-full">
              <div className="bg-gray-100 dark:bg-gray-800 border-b border-gray-100 dark:border-gray-800 px-6 py-3 flex items-center text-xs font-medium text-gray-900 dark:text-gray-200 uppercase tracking-wider">
                <div className="flex-1">Address</div>
                <div className="w-32 text-center">Status</div>
                <div className="w-32 text-right">Actions</div>
              </div>

              <motion.div 
                className="divide-y divide-gray-100 dark:divide-gray-800"
                initial="hidden"
                animate="show"
                variants={{
                  hidden: { opacity: 0 },
                  show: {
                    opacity: 1,
                    transition: {
                      staggerChildren: 0.1
                    }
                  }
                }}
              >
                <AnimatePresence mode="popLayout">
                  {nameServers.map((ns) => (
                    <motion.div 
                      layout
                      key={ns}
                      variants={{
                        hidden: { opacity: 0, y: 10 },
                        show: { opacity: 1, y: 0 }
                      }}
                      exit={{ opacity: 0, height: 0, marginBottom: 0, transition: { duration: 0.2 } }}
                      whileHover={{ scale: 1.002 }}
                      transition={{ type: "spring", stiffness: 400, damping: 30 }}
                      className={`flex items-center px-6 py-4 transition-colors relative group hover:bg-gray-50 dark:hover:bg-gray-800/50 ${ns === selectedNameServer ? 'bg-gray-100 dark:bg-gray-800' : 'bg-white dark:bg-gray-900'}`}
                    >
                      {ns === selectedNameServer && (
                        <motion.div 
                          layoutId="active-row-indicator"
                          className="absolute left-0 top-0 bottom-0 w-1 bg-blue-500" 
                        />
                      )}

                      <div className="flex-1 font-mono text-sm text-gray-700 dark:text-gray-300 flex items-center">
                        <Server className={`w-4 h-4 mr-3 ${ns === selectedNameServer ? 'text-blue-500' : 'text-gray-400 dark:text-gray-500'}`} />
                        {ns}
                      </div>

                      <div className="w-32 flex justify-center">
                        {ns === selectedNameServer ? (
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-400 border border-green-200 dark:border-green-800 shadow-sm">
                            <motion.span 
                              animate={{ scale: [1, 1.2, 1], opacity: [1, 0.5, 1] }}
                              transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
                              className="w-1.5 h-1.5 rounded-full bg-green-500 mr-1.5"
                            />
                            Active
                          </span>
                        ) : (
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400 border border-gray-200 dark:border-gray-700">
                            Standby
                          </span>
                        )}
                      </div>

                      <div className="w-32 flex justify-end items-center space-x-2 opacity-0 group-hover:opacity-100 transition-opacity duration-200">
                        {ns !== selectedNameServer && (
                          <motion.button 
                            whileHover={{ scale: 1.05 }}
                            whileTap={{ scale: 0.95 }}
                            onClick={() => handleSwitchServer(ns)}
                            className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 px-3 py-1.5 rounded-md hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors"
                          >
                            Switch
                          </motion.button>
                        )}
                        <motion.button 
                          whileHover={{ scale: 1.1, color: "#ef4444" }}
                          whileTap={{ scale: 0.9 }}
                          onClick={() => handleRemoveNameServer(ns)}
                          className={`p-1.5 rounded-md transition-colors ${
                            ns === selectedNameServer 
                              ? 'text-gray-300 dark:text-gray-700 cursor-not-allowed' 
                              : 'text-gray-400 dark:text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800'
                          }`}
                          disabled={ns === selectedNameServer}
                        >
                          <Trash2 className="w-4 h-4" />
                        </motion.button>
                      </div>
                    </motion.div>
                  ))}
                </AnimatePresence>
              </motion.div>
            </div>
          </div>

        </div>
      </Card>

      <Card title="Connection Security" description="Configure secure transport layers.">
        <div className="divide-y divide-gray-100 dark:divide-gray-800">
          <div className="flex items-center justify-between py-4">
            <span className="text-sm font-medium text-gray-900 dark:text-gray-300">VIP Channel</span>
            <Toggle checked={isVIPChannel} onChange={setIsVIPChannel} />
          </div>
          <div className="flex items-center justify-between py-4">
             <span className="text-sm font-medium text-gray-900 dark:text-gray-300">TLS Encryption</span>
             <Toggle checked={useTLS} onChange={setUseTLS} />
          </div>
        </div>
      </Card>
    </div>
  );
};
