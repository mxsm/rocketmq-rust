import React, { useState } from 'react';
import { 
  Shield, 
  Search, 
  Plus, 
  Eye, 
  EyeOff, 
  MoreHorizontal, 
  Database, 
  X, 
  Check, 
  Server, 
  User, 
  Lock,
  Filter
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { toast } from 'sonner@2.0.3';

// Internal Card Component to match the style
const Card = ({ children, className = "", title, description, headerAction }: any) => (
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

// Reusable Table Component
const Table = ({ headers, children }: { headers: string[], children: React.ReactNode }) => (
  <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-800">
    <table className="w-full text-left text-sm">
      <thead className="bg-gray-50 dark:bg-gray-800/50">
        <tr>
          {headers.map((header, i) => (
            <th key={i} className="px-6 py-3 font-medium text-gray-500 dark:text-gray-400 whitespace-nowrap">
              {header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
        {children}
      </tbody>
    </table>
  </div>
);

// Empty State Component
const EmptyState = ({ message }: { message: string }) => (
  <div className="flex flex-col items-center justify-center py-12 text-gray-400">
    <div className="w-16 h-16 mb-4 rounded-full bg-gray-100 dark:bg-gray-800 flex items-center justify-center">
      <Database className="w-8 h-8 opacity-40" />
    </div>
    <p className="text-sm font-medium text-gray-500 dark:text-gray-400">{message}</p>
  </div>
);

export const ACLView = () => {
  const [activeTab, setActiveTab] = useState<'users' | 'permissions'>('users');
  const [showAddUserModal, setShowAddUserModal] = useState(false);
  const [showAddPermissionModal, setShowAddPermissionModal] = useState(false);
  
  // Form States
  const [passwordVisible, setPasswordVisible] = useState(false);

  return (
    <div className="space-y-6">
      {/* Top Controls Card */}
      <Card>
        <div className="flex flex-wrap items-center gap-4">
          <div className="flex items-center space-x-2 flex-1 min-w-[200px]">
            <span className="text-sm text-gray-500 whitespace-nowrap">Cluster:</span>
            <select className="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:text-white">
              <option>DefaultCluster</option>
            </select>
          </div>
          <div className="flex items-center space-x-2 flex-1 min-w-[200px]">
             <span className="text-sm text-gray-500 whitespace-nowrap">Broker:</span>
             <select className="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:text-white">
              <option>mxsm</option>
              <option>broker-a</option>
            </select>
          </div>
          <button className="px-6 py-2 bg-gray-900 dark:bg-white text-white dark:text-gray-900 text-sm font-medium rounded-lg hover:opacity-90 transition-opacity shadow-sm">
            Confirm
          </button>
        </div>
      </Card>

      {/* Main Content Area */}
      <div className="bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm overflow-hidden min-h-[500px] flex flex-col">
        {/* Tabs */}
        <div className="flex border-b border-gray-100 dark:border-gray-800 px-6 pt-2">
          <button
            onClick={() => setActiveTab('users')}
            className={`mr-8 pb-4 text-sm font-medium transition-colors relative ${
              activeTab === 'users' 
                ? 'text-blue-600 dark:text-blue-400' 
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            ACL Users
            {activeTab === 'users' && (
              <motion.div 
                layoutId="activeTab"
                className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 dark:bg-blue-400" 
              />
            )}
          </button>
          <button
            onClick={() => setActiveTab('permissions')}
            className={`pb-4 text-sm font-medium transition-colors relative ${
              activeTab === 'permissions' 
                ? 'text-blue-600 dark:text-blue-400' 
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            ACL Permissions
            {activeTab === 'permissions' && (
              <motion.div 
                layoutId="activeTab"
                className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 dark:bg-blue-400" 
              />
            )}
          </button>
        </div>

        {/* Tab Content */}
        <div className="p-6 flex-1">
          {activeTab === 'users' ? (
            <div className="space-y-4">
              <div className="flex justify-between items-center">
                <button 
                  onClick={() => setShowAddUserModal(true)}
                  className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm flex items-center"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add User
                </button>
              </div>
              
              <Table headers={['Username', 'Password', 'User Type', 'User Status', 'Operation']}>
                {/* Empty State or Data Mapping */}
                <tr>
                  <td colSpan={5} className="py-12">
                    <EmptyState message="No users found" />
                  </td>
                </tr>
              </Table>
            </div>
          ) : (
             <div className="space-y-4">
              <div className="flex justify-between items-center">
                <button 
                  onClick={() => setShowAddPermissionModal(true)}
                  className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors shadow-sm flex items-center"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add ACL Permission
                </button>
                
                <div className="relative">
                  <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
                  <input 
                    type="text" 
                    placeholder="Search..." 
                    className="pl-9 pr-4 py-2 w-64 text-sm border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 dark:text-white placeholder-gray-400"
                  />
                </div>
              </div>

              <Table headers={['Username/Subject', 'Policy Type', 'Resource Name', 'Operation Type', 'Source IP', 'Decision', 'Operation']}>
                {/* Empty State or Data Mapping */}
                <tr>
                  <td colSpan={7} className="py-12">
                     <EmptyState message="No permissions found" />
                  </td>
                </tr>
              </Table>
            </div>
          )}
        </div>
      </div>

      {/* Add User Modal */}
      <AnimatePresence>
        {showAddUserModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 0.5 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowAddUserModal(false)}
              className="absolute inset-0 bg-black"
            />
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 10 }}
              className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl border border-gray-100 dark:border-gray-800 overflow-hidden"
            >
              <div className="px-6 py-4 border-b border-gray-100 dark:border-gray-800 flex justify-between items-center">
                <h3 className="text-lg font-bold text-gray-900 dark:text-white">Add User</h3>
                <button onClick={() => setShowAddUserModal(false)} className="text-gray-400 hover:text-gray-600">
                  <X className="w-5 h-5" />
                </button>
              </div>
              
              <div className="p-6 space-y-4">
                <div className="space-y-1.5">
                  <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Username
                  </label>
                  <input type="text" className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white" />
                </div>
                
                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Password
                  </label>
                  <div className="relative">
                    <input 
                      type={passwordVisible ? "text" : "password"} 
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white pr-10" 
                      placeholder="Password"
                    />
                    <button 
                      type="button"
                      onClick={() => setPasswordVisible(!passwordVisible)}
                      className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
                    >
                      {passwordVisible ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                    </button>
                  </div>
                </div>
                
                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>User Type
                  </label>
                  <select className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white">
                    <option>Super</option>
                    <option>Normal</option>
                  </select>
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>User Status
                  </label>
                  <select className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white">
                    <option>enable</option>
                    <option>disable</option>
                  </select>
                </div>
              </div>

              <div className="px-6 py-4 bg-gray-50 dark:bg-gray-800/50 flex justify-end space-x-3">
                <button 
                  onClick={() => setShowAddUserModal(false)}
                  className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                >
                  Cancel
                </button>
                <button 
                  onClick={() => {
                    toast.success("User added successfully");
                    setShowAddUserModal(false);
                  }}
                  className="px-4 py-2 bg-gray-900 dark:bg-blue-600 text-white text-sm font-medium rounded-lg hover:opacity-90 transition-opacity"
                >
                  Confirm
                </button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Add Permission Modal */}
      <AnimatePresence>
        {showAddPermissionModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 0.5 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowAddPermissionModal(false)}
              className="absolute inset-0 bg-black"
            />
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 10 }}
              className="relative w-full max-w-2xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl border border-gray-100 dark:border-gray-800 overflow-hidden max-h-[90vh] flex flex-col"
            >
              <div className="px-6 py-4 border-b border-gray-100 dark:border-gray-800 flex justify-between items-center">
                <h3 className="text-lg font-bold text-gray-900 dark:text-white">Add ACL Permission</h3>
                <button onClick={() => setShowAddPermissionModal(false)} className="text-gray-400 hover:text-gray-600">
                  <X className="w-5 h-5" />
                </button>
              </div>
              
              <div className="p-6 space-y-5 overflow-y-auto">
                <div className="space-y-1.5">
                  <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Subject (e.g.: User:yourUsername)
                  </label>
                  <div className="flex gap-2">
                    <select className="w-32 px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white">
                       <option>User</option>
                       <option>Role</option>
                    </select>
                    <input 
                      type="text" 
                      placeholder="Please input name"
                      className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white" 
                    />
                  </div>
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Policy Type
                  </label>
                  <input 
                      type="text" 
                      placeholder="policyType"
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white" 
                  />
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Resource
                  </label>
                  <div className="border border-dashed border-gray-300 dark:border-gray-700 rounded-lg p-2">
                     <button className="flex items-center text-sm text-gray-500 hover:text-blue-600 px-2 py-1">
                        <Plus className="w-4 h-4 mr-1" />
                        Add Resource
                     </button>
                  </div>
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    Operation Type
                  </label>
                  <select className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white">
                    <option>action</option>
                    <option>read</option>
                    <option>write</option>
                  </select>
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    Source IP
                  </label>
                  <input 
                      type="text" 
                      placeholder="Please enter IP address. Supports IPv4, IPv6..."
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white" 
                  />
                </div>

                <div className="space-y-1.5">
                   <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                    <span className="text-red-500 mr-1">*</span>Decision
                  </label>
                  <select className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-950 focus:ring-2 focus:ring-blue-500 outline-none text-sm dark:text-white">
                    <option>Allow</option>
                    <option>Deny</option>
                  </select>
                </div>
              </div>

              <div className="px-6 py-4 bg-gray-50 dark:bg-gray-800/50 flex justify-end space-x-3 mt-auto">
                <button 
                  onClick={() => setShowAddPermissionModal(false)}
                  className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                >
                  Cancel
                </button>
                <button 
                  onClick={() => {
                     toast.success("Permission added successfully");
                     setShowAddPermissionModal(false);
                  }}
                  className="px-4 py-2 bg-gray-900 dark:bg-blue-600 text-white text-sm font-medium rounded-lg hover:opacity-90 transition-opacity"
                >
                  OK
                </button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
};
