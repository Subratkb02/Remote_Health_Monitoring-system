export interface HealthData {
  bmp_temp: {
    N: string;
  };
  probe_temp: {
    N: string;
  };
  pressure: {
    N: string;
  };
  timestamp?: number;
}

export interface DataPoint {
  value: number;
  timestamp: number;
}

export interface HistoricalData {
  bmpTemp: DataPoint[];
  probeTemp: DataPoint[];
  pressure: DataPoint[];
}

export interface ConnectionStatus {
  connected: boolean;
  error: string | null;
  lastUpdated: number | null;
}

// Normal ranges for health metrics
export const HEALTH_RANGES = {
  bmpTemp: { min: 25, max: 32 },
  probeTemp: { min: 27, max: 34 },
  pressure: { min: 900, max: 1000 }
};

// Maximum number of historical data points to keep
export const MAX_DATA_POINTS = 20;