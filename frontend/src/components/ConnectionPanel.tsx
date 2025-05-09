import React, { useState } from 'react';
import { Wifi, WifiOff, AlertCircle } from 'lucide-react';
import { useWebSocket } from '../contexts/WebSocketContext';
import { format } from 'date-fns';

const ConnectionPanel: React.FC = () => {
  const { ipAddress, setIpAddress, connectionStatus, connect, disconnect } = useWebSocket();
  const [showDetails, setShowDetails] = useState(false);

  const handleConnect = () => {
    if (connectionStatus.connected) {
      disconnect();
    } else {
      connect();
    }
  };

  const formatTimestamp = (timestamp: number | null) => {
    if (!timestamp) return 'N/A';
    return format(new Date(timestamp), 'HH:mm:ss');
  };

  return (
    <div className="card mb-6 bg-white">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center">
        <div className="flex items-center mb-4 md:mb-0 w-full md:w-auto">
          <div className="flex items-center mr-4">
            {connectionStatus.connected ? (
              <Wifi size={20} className="text-success-500 mr-2" />
            ) : (
              <WifiOff size={20} className="text-gray-400 mr-2" />
            )}
            <span className="font-medium">
              {connectionStatus.connected ? 'Connected' : 'Disconnected'}
            </span>
            
            {connectionStatus.connected && (
              <div className="ml-2 flex items-center">
                <div className="w-2 h-2 bg-success-500 rounded-full animate-pulse mr-1"></div>
                <span className="text-xs text-success-600">Live</span>
              </div>
            )}
          </div>
          
          {connectionStatus.error && (
            <div className="flex items-center text-error-500">
              <AlertCircle size={16} className="mr-1" />
              <span className="text-sm">{connectionStatus.error}</span>
            </div>
          )}
        </div>
        
        <div className="flex flex-col md:flex-row items-start md:items-center space-y-2 md:space-y-0 md:space-x-3 w-full md:w-auto">
          <div className="w-full md:w-64">
            <input
              type="text"
              placeholder="WebSocket IP (e.g., 192.168.1.1:8080)"
              value={ipAddress}
              onChange={(e) => setIpAddress(e.target.value)}
              className="input"
              disabled={connectionStatus.connected}
            />
          </div>
          
          <button
            onClick={handleConnect}
            className={`btn ${connectionStatus.connected ? 'btn-outline border-error-300 text-error-500' : 'btn-primary'} whitespace-nowrap`}
          >
            {connectionStatus.connected ? 'Disconnect' : 'Connect'}
          </button>
          
          <button 
            className="text-sm text-primary-500 underline md:ml-2"
            onClick={() => setShowDetails(!showDetails)}
          >
            {showDetails ? 'Hide Details' : 'Show Details'}
          </button>
        </div>
      </div>
      
      {showDetails && (
        <div className="mt-4 text-sm text-gray-600 border-t pt-4">
          <p><span className="font-semibold">Status:</span> {connectionStatus.connected ? 'Connected' : 'Disconnected'}</p>
          <p><span className="font-semibold">Last updated:</span> {formatTimestamp(connectionStatus.lastUpdated)}</p>
          <p><span className="font-semibold">WebSocket URL:</span> {ipAddress ? `ws://${ipAddress}` : 'Not set'}</p>
          {connectionStatus.error && <p><span className="font-semibold">Error:</span> {connectionStatus.error}</p>}
        </div>
      )}
    </div>
  );
};

export default ConnectionPanel;
