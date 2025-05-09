import React, { useState } from 'react';
import { format } from 'date-fns';
import { useHealthData } from '../contexts/HealthDataContext';
import { ArrowUpDown } from 'lucide-react';

const HistoricalDataTable: React.FC = () => {
  const { historicalData } = useHealthData();
  const [sortConfig, setSortConfig] = useState<{
    key: string;
    direction: 'ascending' | 'descending';
  }>({
    key: 'timestamp',
    direction: 'descending',
  });

  // Combine all data points into a single array for the table
  const tableData = historicalData.bmpTemp.map((point, index) => {
    const probeTempPoint = historicalData.probeTemp[index] || { value: null, timestamp: point.timestamp };
    const pressurePoint = historicalData.pressure[index] || { value: null, timestamp: point.timestamp };

    return {
      id: index,
      timestamp: point.timestamp,
      bmpTemp: point.value,
      probeTemp: probeTempPoint.value,
      pressure: pressurePoint.value,
    };
  });

  // Sort the data
  const sortedData = [...tableData].sort((a, b) => {
    if (a[sortConfig.key as keyof typeof a] === null) return 1;
    if (b[sortConfig.key as keyof typeof b] === null) return -1;
    
    if (sortConfig.key === 'timestamp') {
      return sortConfig.direction === 'ascending'
        ? a.timestamp - b.timestamp
        : b.timestamp - a.timestamp;
    }
    
    // For numerical values
    const aValue = a[sortConfig.key as keyof typeof a];
    const bValue = b[sortConfig.key as keyof typeof b];
    
    if (aValue === null || bValue === null) return 0;
    
    return sortConfig.direction === 'ascending'
      ? (aValue as number) - (bValue as number)
      : (bValue as number) - (aValue as number);
  });

  const requestSort = (key: string) => {
    let direction: 'ascending' | 'descending' = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  return (
    <div className="card overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th 
                scope="col" 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
                onClick={() => requestSort('timestamp')}
              >
                <div className="flex items-center">
                  Timestamp
                  <ArrowUpDown size={14} className="ml-1" />
                </div>
              </th>
              <th 
                scope="col" 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
                onClick={() => requestSort('bmpTemp')}
              >
                <div className="flex items-center">
                  BMP Temp (°C)
                  <ArrowUpDown size={14} className="ml-1" />
                </div>
              </th>
              <th 
                scope="col" 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
                onClick={() => requestSort('probeTemp')}
              >
                <div className="flex items-center">
                  Probe Temp (°C)
                  <ArrowUpDown size={14} className="ml-1" />
                </div>
              </th>
              <th 
                scope="col" 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
                onClick={() => requestSort('pressure')}
              >
                <div className="flex items-center">
                  Pressure (hPa)
                  <ArrowUpDown size={14} className="ml-1" />
                </div>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedData.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-4 text-center text-sm text-gray-500">
                  No data available
                </td>
              </tr>
            ) : (
              sortedData.map((row) => (
                <tr key={row.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {format(new Date(row.timestamp), 'yyyy-MM-dd HH:mm:ss')}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {row.bmpTemp !== null ? row.bmpTemp.toFixed(2) : '--'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {row.probeTemp !== null ? row.probeTemp.toFixed(2) : '--'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {row.pressure !== null ? row.pressure.toFixed(2) : '--'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default HistoricalDataTable;