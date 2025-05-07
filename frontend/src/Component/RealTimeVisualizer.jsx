import { useEffect, useState, useRef } from "react";
import PropTypes from "prop-types";
import { Line } from "react-chartjs-2";
import { Chart, Filler } from 'chart.js';
Chart.register(Filler);
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

function RealTimeVisualizer({ title, chartData }) {
  const chartRef = useRef(null);
  const [dataPoints, setDataPoints] = useState([]);
  const MAX_DATA_POINTS = 30; // Show more data points for a better visualization

  // Handle incoming data
  useEffect(() => {
    if (typeof chartData === "number") {
      setDataPoints(prevData => {
        const newData = [...prevData, chartData];
        // Keep only the most recent MAX_DATA_POINTS entries
        return newData.length > MAX_DATA_POINTS ? newData.slice(-MAX_DATA_POINTS) : newData;
      });
    } else if (Array.isArray(chartData) && chartData.length > 0) {
      // Handle case where chartData is an array (from your updated Dashboard component)
      const latestValue = chartData[chartData.length - 1];
      if (typeof latestValue === "number") {
        setDataPoints(prevData => {
          const newData = [...prevData, latestValue];
          return newData.length > MAX_DATA_POINTS ? newData.slice(-MAX_DATA_POINTS) : newData;
        });
      }
    }
  }, [chartData]);

  // Determine y-axis range based on data type
  const getYAxisConfig = () => {
    switch(title.toLowerCase()) {
      case "Body temperature:":
        return { min: 25, max: 45 }; // Common body temperature range in Celsius
      case "Pressure:":
        return { min: 30, max: 180 }; // Heart rate range in BPM
      default:
        return { min: 0, max: 120 };
    }
  };

  const yAxisConfig = getYAxisConfig();
  
  // Create chart data structure
  const data = {
    labels: Array.from({ length: dataPoints.length }, (_, i) => i + 1),
    datasets: [
      {
        label: title.replace(":", ""),
        data: dataPoints,
        fill: true,
        backgroundColor: "rgba(26, 26, 26, 0.3)", // Semi-transparent background
        borderColor: "rgba(255, 140, 0, 0.8)", // Orange border color
        borderWidth: 4,
        tension: 0.3,
        pointRadius: 2, // Smaller points for smoother appearance
        pointHoverRadius: 6,
      },
    ],
  };

  // Animation configuration
  const animation = {
    duration: 0, // Instant updates for real-time feel
  };

  return (
    <div
      style={{
        width: "90%",
        maxWidth: "600px",
        height: "300px", // Fixed height for better chart proportions
        margin: "20px auto",
        backgroundColor: "#1a1a1a",
        padding: "20px",
        borderRadius: "15px",
        boxShadow: "0 6px 12px rgba(255, 140, 0, 0.3)",
        transition: "transform 0.4s, box-shadow 0.4s",
        overflow: "hidden",
      }}
      className="visualizer-card"
    >
      <h2
        style={{
          color: "#f5f5f5",
          textAlign: "center",
          marginBottom: "15px",
        }}
      >
        {`${title} ${dataPoints.length > 0 ? dataPoints[dataPoints.length - 1].toFixed(1) : "N/A"}`}
      </h2>
      <div style={{ position: "relative", width: "100%", height: "220px" }}>
        <Line
          ref={chartRef}
          data={data}
          options={{
            responsive: true,
            maintainAspectRatio: false,
            animation,
            scales: {
              x: {
                display: false,
                grid: {
                  color: "rgba(255, 255, 255, 0.1)", // Subtle grid lines
                },
              },
              y: {
                display: true,
                min: yAxisConfig.min,
                max: yAxisConfig.max,
                ticks: {
                  color: "#f5f5f5",
                  font: {
                    size: 10, // Smaller font for y-axis
                  },
                },
                grid: {
                  color: "rgba(255, 255, 255, 0.1)", // Subtle grid lines
                },
              },
            },
            plugins: {
              legend: {
                display: false,
              },
              tooltip: {
                backgroundColor: "rgba(255, 140, 0, 0.8)",
                titleColor: "#fff",
                bodyColor: "#fff",
                borderColor: "#fff",
                borderWidth: 1,
                callbacks: {
                  label: function(context) {
                    return `${context.dataset.label}: ${context.parsed.y.toFixed(2)}`;
                  }
                }
              },
            },
          }}
        />
      </div>
    </div>
  );
}
RealTimeVisualizer.propTypes = {
  title: PropTypes.string.isRequired,
  chartData: PropTypes.oneOfType([
    PropTypes.number,
    PropTypes.arrayOf(PropTypes.number),
  ]).isRequired,
};

export default RealTimeVisualizer;