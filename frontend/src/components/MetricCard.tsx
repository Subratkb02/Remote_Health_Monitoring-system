import React, { useState, useEffect, ReactNode } from 'react';
import { ArrowUp, ArrowDown, AlertTriangle } from 'lucide-react';
import { HEALTH_RANGES } from '../types';

interface MetricCardProps {
  title: string;
  value: string;
  unit: string;
  icon: ReactNode;
  status: 'normal' | 'warning' | 'critical';
  range: string;
}

const MetricCard: React.FC<MetricCardProps> = ({ 
  title, 
  value, 
  unit, 
  icon,
  status: initialStatus,
  range 
}) => {
  const [prevValue, setPrevValue] = useState<number | null>(null);
  const [trend, setTrend] = useState<'up' | 'down' | null>(null);
  const [status, setStatus] = useState<'normal' | 'warning' | 'critical'>(initialStatus);

  // Calculate if value is in normal range
  useEffect(() => {
    if (value === '--') {
      setStatus('normal');
      return;
    }
    
    const numericValue = parseFloat(value);
    
    // Determine the range based on the title
    let rangeKey: keyof typeof HEALTH_RANGES;
    
    if (title.includes('BMP')) {
      rangeKey = 'bmpTemp';
    } else if (title.includes('Probe')) {
      rangeKey = 'probeTemp';
    } else {
      rangeKey = 'pressure';
    }
    
    const { min, max } = HEALTH_RANGES[rangeKey];
    
    if (numericValue < min || numericValue > max) {
      setStatus('warning');
      
      // If significantly out of range, mark as critical
      if (numericValue < min * 0.9 || numericValue > max * 1.1) {
        setStatus('critical');
      }
    } else {
      setStatus('normal');
    }
    
    // Set trend direction
    if (prevValue !== null) {
      if (numericValue > prevValue) {
        setTrend('up');
      } else if (numericValue < prevValue) {
        setTrend('down');
      }
      
      // Reset trend after 2 seconds
      const timeout = setTimeout(() => {
        setTrend(null);
      }, 2000);
      
      return () => clearTimeout(timeout);
    }
    
    setPrevValue(numericValue);
  }, [value, title, prevValue]);

  // Get the appropriate color based on status
  const getStatusColor = () => {
    switch (status) {
      case 'critical':
        return 'bg-error-50 border-error-200';
      case 'warning':
        return 'bg-warning-50 border-warning-200';
      default:
        return 'bg-white';
    }
  };

  return (
    <div className={`card border ${getStatusColor()} transition-colors duration-300`}>
      <div className="flex justify-between items-start mb-3">
        <h3 className="font-medium text-gray-700">{title}</h3>
        <div className="text-secondary-500">{icon}</div>
      </div>
      
      <div className="flex items-end">
        <div className="text-3xl font-bold mr-2 flex items-center">
          {value}
          <span className="text-lg ml-1 font-normal text-gray-500">{unit}</span>
          
          {trend === 'up' && (
            <ArrowUp size={20} className="ml-2 text-success-500" />
          )}
          
          {trend === 'down' && (
            <ArrowDown size={20} className="ml-2 text-error-500" />
          )}
        </div>
      </div>
      
      <div className="mt-4 flex justify-between items-center text-sm text-gray-500">
        <div>Normal range: {range}</div>
        
        {status !== 'normal' && (
          <div className={`flex items-center ${status === 'critical' ? 'text-error-500 animate-pulse' : 'text-warning-500'}`}>
            <AlertTriangle size={16} className="mr-1" />
            <span>{status === 'critical' ? 'Critical' : 'Warning'}</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default MetricCard;