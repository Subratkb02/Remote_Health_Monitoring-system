import React from 'react';
import { Activity, Heart, Thermometer, Gauge } from 'lucide-react';
import ConnectionPanel from './ConnectionPanel';
import MetricCard from './MetricCard';
import ChartPanel from './ChartPanel';
import HistoricalDataTable from './HistoricalDataTable';
import { useHealthData } from '../contexts/HealthDataContext';
import { useWebSocket } from '../contexts/WebSocketContext';

const Dashboard: React.FC = () => {
  const { currentData, exportData, clearData } = useHealthData();
  const { connectionStatus } = useWebSocket();
  
  return (
    <div className="min-h-screen p-4 md:p-6">
      <header className="mb-8">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-4">
          <div className="flex items-center mb-4 md:mb-0">
            <Activity size={32} className="text-primary-500 mr-3" />
            <h1 className="text-2xl md:text-3xl font-bold text-gray-800">
              Remote Healthcare Monitoring
            </h1>
          </div>
          
          <div className="flex items-center space-x-2">
            {connectionStatus.connected && (
              <>
                <button 
                  onClick={exportData}
                  className="btn btn-secondary"
                >
                  Export Data
                </button>
                <button 
                  onClick={clearData}
                  className="btn btn-outline"
                >
                  Clear Data
                </button>
              </>
            )}
          </div>
        </div>
        
        <ConnectionPanel />
      </header>
      
      <main>
        {/* Current Metrics Section */}
        <section className="mb-8">
          <h2 className="text-xl font-semibold mb-4 text-gray-700">Current Readings</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <MetricCard 
              title="BMP Temperature" 
              value={currentData?.bmp_temp.N || '--'} 
              unit="°C"
              icon={<Thermometer className="text-primary-500" />}
              status="normal"
              range="25-32°C"
            />
            <MetricCard 
              title="Probe Temperature" 
              value={currentData?.probe_temp.N || '--'} 
              unit="°C"
              icon={<Heart className="text-error-500" />}
              status="normal"
              range="27-34°C"
            />
            <MetricCard 
              title="Pressure" 
              value={currentData?.pressure.N || '--'} 
              unit="hPa"
              icon={<Gauge className="text-secondary-500" />}
              status="normal"
              range="900-1000 hPa"
            />
          </div>
        </section>
        
        {/* Charts Section */}
        <section className="mb-8">
          <h2 className="text-xl font-semibold mb-4 text-gray-700">Trend Analysis</h2>
          <ChartPanel />
        </section>
        
        {/* Historical Data Section */}
        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-700">Historical Data</h2>
          <HistoricalDataTable />
        </section>
      </main>
      
      <footer className="mt-12 pt-6 border-t border-gray-200 text-center text-gray-500 text-sm">
        <p>© {new Date().getFullYear()} Remote Healthcare Monitoring System</p>
      </footer>
    </div>
  );
};

export default Dashboard;