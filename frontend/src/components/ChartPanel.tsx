import React, { useState } from 'react';
import { Line } from 'react-chartjs-2';
import { format } from 'date-fns';
import { 
  Chart as ChartJS, 
  CategoryScale, 
  LinearScale, 
  PointElement, 
  LineElement, 
  Title, 
  Tooltip, 
  Legend,
  Filler
} from 'chart.js';
import { useHealthData } from '../contexts/HealthDataContext';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const ChartPanel: React.FC = () => {
  const { historicalData } = useHealthData();
  const [activeTab, setActiveTab] = useState<'all' | 'temperature' | 'pressure'>('all');
  
  const formatTime = (timestamp: number) => {
    return format(new Date(timestamp), 'HH:mm:ss');
  };
  
  // Prepare data for temperature chart
  const temperatureData = {
    labels: historicalData.bmpTemp.map(point => formatTime(point.timestamp)),
    datasets: [
      {
        label: 'BMP Temperature (°C)',
        data: historicalData.bmpTemp.map(point => point.value),
        borderColor: 'rgb(53, 162, 235)',
        backgroundColor: 'rgba(53, 162, 235, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 3,
      },
      {
        label: 'Probe Temperature (°C)',
        data: historicalData.probeTemp.map(point => point.value),
        borderColor: 'rgb(255, 99, 132)',
        backgroundColor: 'rgba(255, 99, 132, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 3,
      }
    ]
  };
  
  // Prepare data for pressure chart
  const pressureData = {
    labels: historicalData.pressure.map(point => formatTime(point.timestamp)),
    datasets: [
      {
        label: 'Pressure (hPa)',
        data: historicalData.pressure.map(point => point.value),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 3,
      }
    ]
  };
  
  // Combined chart options
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 700,
      easing: 'easeOutQuart',
    },
    scales: {
      y: {
        beginAtZero: false,
        grid: {
          color: 'rgba(0, 0, 0, 0.05)',
        },
      },
      x: {
        grid: {
          display: false,
        },
      },
    },
    plugins: {
      legend: {
        position: 'top' as const,
      },
      tooltip: {
        backgroundColor: 'rgba(255, 255, 255, 0.9)',
        titleColor: '#333',
        bodyColor: '#666',
        borderColor: 'rgba(0, 0, 0, 0.1)',
        borderWidth: 1,
        padding: 10,
        boxPadding: 5,
        usePointStyle: true,
        callbacks: {
          label: function(context: any) {
            let label = context.dataset.label || '';
            if (label) {
              label += ': ';
            }
            if (context.parsed.y !== null) {
              label += context.parsed.y.toFixed(2);
            }
            return label;
          }
        }
      },
    },
  };
  
  return (
    <div className="card">
      <div className="flex mb-4 border-b">
        <button
          className={`px-4 py-2 font-medium text-sm ${
            activeTab === 'all' ? 'text-primary-500 border-b-2 border-primary-500' : 'text-gray-500 hover:text-gray-700'
          }`}
          onClick={() => setActiveTab('all')}
        >
          All Metrics
        </button>
        <button
          className={`px-4 py-2 font-medium text-sm ${
            activeTab === 'temperature' ? 'text-primary-500 border-b-2 border-primary-500' : 'text-gray-500 hover:text-gray-700'
          }`}
          onClick={() => setActiveTab('temperature')}
        >
          Temperature
        </button>
        <button
          className={`px-4 py-2 font-medium text-sm ${
            activeTab === 'pressure' ? 'text-primary-500 border-b-2 border-primary-500' : 'text-gray-500 hover:text-gray-700'
          }`}
          onClick={() => setActiveTab('pressure')}
        >
          Pressure
        </button>
      </div>
      
      <div className="h-80 flex flex-col">
        {historicalData.bmpTemp.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center text-gray-400">
            <p className="mb-2">No data available</p>
            <p className="text-sm">Connect to a WebSocket to start receiving data</p>
          </div>
        ) : (
          <>
            {(activeTab === 'all' || activeTab === 'temperature') && (
              <div className={`flex-1 ${activeTab === 'all' ? 'mb-4' : ''} relative`}>
                <Line data={temperatureData} options={options} />
              </div>
            )}
            
            {(activeTab === 'all' || activeTab === 'pressure') && (
              <div className="flex-1 relative">
                <Line data={pressureData} options={options} />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};

export default ChartPanel;