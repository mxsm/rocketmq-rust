import React from 'react';
import { 
  RefreshCw
} from 'lucide-react';
import { 
  AreaChart, 
  Area, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  BarChart,
  Bar,
  LineChart,
  Line
} from 'recharts';
import { Card } from '../../../components/ui/LegacyCard';

// Mock Data for Charts
const BROKER_DATA = [
  { name: 'mixsim0', messages: 1200000 },
  { name: 'broker-a', messages: 950000 },
  { name: 'broker-b', messages: 850000 },
  { name: 'broker-c', messages: 600000 },
  { name: 'broker-d', messages: 450000 },
];

const TOPIC_DATA = [
  { name: 'OrderPlaced', count: 4500 },
  { name: 'UserSignup', count: 3200 },
  { name: 'PaymentProcessed', count: 2800 },
  { name: 'EmailSent', count: 2100 },
  { name: 'LogEvent', count: 1800 },
];

const TREND_DATA = Array.from({ length: 12 }).map((_, i) => ({
  time: `${18 + Math.floor(i/6)}:${(i%6)*10 || '00'}`,
  value: 35 + Math.random() * 10
}));

const TOPIC_TREND_DATA = Array.from({ length: 12 }).map((_, i) => ({
  time: `${18 + Math.floor(i/6)}:${(i%6)*10 || '00'}`,
  value: Math.random() * 2
}));

export const DashboardCharts = () => {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      
      {/* Broker TOP 10 */}
      <Card title="Broker Top 10" description="Highest message volume by broker">
        <div className="mt-4 min-w-0 h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
            <BarChart data={BROKER_DATA} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={true} horizontal={false} stroke="#f0f0f0" />
              <XAxis type="number" hide />
              <YAxis dataKey="name" type="category" width={80} tick={{fontSize: 12}} axisLine={false} tickLine={false} />
              <Tooltip 
                cursor={{fill: '#f9fafb'}}
                contentStyle={{borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)'}}
              />
              <Bar dataKey="messages" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={20} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* Broker Trend */}
      <Card title="Broker 5min Trend" description="Message rate over the last 5 minutes" 
        headerAction={
          <div className="flex space-x-2">
             <button className="p-1.5 hover:bg-gray-100 rounded-md text-gray-400 hover:text-gray-600 transition-colors"><RefreshCw className="w-4 h-4" /></button>
          </div>
        }
      >
        <div className="mt-4 min-w-0 h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
            <AreaChart data={TREND_DATA}>
              <defs>
                <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ef4444" stopOpacity={0.1}/>
                  <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
              <XAxis dataKey="time" tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <YAxis tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <Tooltip 
                contentStyle={{borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)'}}
              />
              <Area type="monotone" dataKey="value" stroke="#ef4444" strokeWidth={2} fillOpacity={1} fill="url(#colorValue)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* Topic TOP 10 */}
      <Card title="Topic Top 10" description="Most active topics by message count">
        <div className="mt-4 min-w-0 h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
            <BarChart data={TOPIC_DATA}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
              <XAxis dataKey="name" tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <YAxis tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <Tooltip 
                 cursor={{fill: '#f9fafb'}}
                 contentStyle={{borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)'}}
              />
              <Bar dataKey="count" fill="#6366f1" radius={[4, 4, 0, 0]} barSize={30} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* Topic Trend */}
      <Card title="Topic 5min Trend" description="Topic activity trend analysis">
        <div className="mt-4 min-w-0 h-[300px] w-full">
           <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
            <LineChart data={TOPIC_TREND_DATA}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
              <XAxis dataKey="time" tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <YAxis tick={{fontSize: 11, fill: '#9ca3af'}} axisLine={false} tickLine={false} />
              <Tooltip 
                contentStyle={{borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)'}}
              />
              <Line type="stepAfter" dataKey="value" stroke="#3b82f6" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

    </div>
  );
};
