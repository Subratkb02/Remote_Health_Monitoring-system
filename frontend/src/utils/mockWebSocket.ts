import { HealthData } from '../types';

// This is a utility to test the dashboard with mock data without a real WebSocket connection
export class MockWebSocket {
  private callbacks: Record<string, Function[]> = {
    message: [],
    open: [],
    close: [],
    error: []
  };
  
  public readyState = WebSocket.OPEN;
  
  constructor() {
    // Simulate connecting
    setTimeout(() => {
      this.triggerEvent('open', {});
      
      // Start sending mock data
      this.startSendingMockData();
    }, 500);
  }
  
  addEventListener(event: string, callback: Function) {
    if (!this.callbacks[event]) {
      this.callbacks[event] = [];
    }
    this.callbacks[event].push(callback);
  }
  
  removeEventListener(event: string, callback: Function) {
    if (this.callbacks[event]) {
      this.callbacks[event] = this.callbacks[event].filter(cb => cb !== callback);
    }
  }
  
  send(data: any) {
    console.log('Mock WebSocket sent:', data);
  }
  
  close() {
    this.readyState = WebSocket.CLOSED;
    this.triggerEvent('close', {});
  }
  
  private triggerEvent(event: string, data: any) {
    if (this.callbacks[event]) {
      this.callbacks[event].forEach(callback => callback(data));
    }
  }
  
  private startSendingMockData() {
    // Starting values
    let bmpTemp = 28.6;
    let probeTemp = 30;
    let pressure = 946.23;
    
    // Send data every 3 seconds
    setInterval(() => {
      // Random small changes to simulate real data
      bmpTemp += (Math.random() - 0.5) * 0.3;
      probeTemp += (Math.random() - 0.5) * 0.2;
      pressure += (Math.random() - 0.5) * 1.5;
      
      // Ensure values stay in reasonable ranges
      bmpTemp = Math.max(25, Math.min(32, bmpTemp));
      probeTemp = Math.max(27, Math.min(34, probeTemp));
      pressure = Math.max(930, Math.min(960, pressure));
      
      const mockData: HealthData = {
        bmp_temp: { N: bmpTemp.toFixed(2) },
        probe_temp: { N: probeTemp.toFixed(2) },
        pressure: { N: pressure.toFixed(2) }
      };
      
      this.triggerEvent('message', { data: JSON.stringify(mockData) });
    }, 3000);
  }
}

// Export function to create a mock WebSocket
export const createMockWebSocket = () => new MockWebSocket();