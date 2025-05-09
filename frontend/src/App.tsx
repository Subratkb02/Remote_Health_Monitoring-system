import Dashboard from './components/Dashboard';
import { WebSocketProvider } from './contexts/WebSocketContext';
import { HealthDataProvider } from './contexts/HealthDataContext';

function App() {
  return (
    <WebSocketProvider>
      <HealthDataProvider>
        <div className="min-h-screen bg-gray-50">
          <Dashboard />
        </div>
      </HealthDataProvider>
    </WebSocketProvider>
  );
}

export default App;