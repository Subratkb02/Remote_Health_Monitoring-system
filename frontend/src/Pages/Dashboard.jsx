// import React, { useEffect } from "react";
// import Header from "../Component/Header";
// import RealTimeVisualizer from "../Component/RealTimeVisualizer";
// import Metrics from "../Component/Metrics";
// import Prediction from "../Component/Prediction";
// import { useAppState } from "../GlobalContext/AppContext"; // Importing context

// function Dashboard() {
//   const {
//     ecg, // Updated from ecgLevels
//     bodyPosture,
//     temperature, // Updated from spo2Level
//     heartRate, // New addition
//     setEcg, // Updated function
//     setBodyPosture,
//     setTemperature, // Updated function
//     setHeartRate, // New addition
//   } = useAppState();

//   // Fetch sensor data from WebSocket
//   const fetchSensorData = () => {
//     const socket = new WebSocket("ws://127.0.0.1:5000");

//     socket.onopen = () => {
//       console.log("WebSocket connection established.");
//     };

//     socket.onmessage = (event) => {
//       const message = JSON.parse(event.data);
//       console.log("Received WebSocket message:", message);

//       if (message.SensorData) {
//         const data = message.SensorData;
//         console.log("SensorData:", data);

//         // Safely update the state only if the set functions are available
//         if (setEcg && setBodyPosture && setTemperature && setHeartRate) {
//           setEcg(data.ecg); // Update ECG
//           let fall = data.fallDetected ? 1 : 0;
//           setBodyPosture(fall); // Update posture
//           setTemperature(data.temperature); // Update Temperature
//           setHeartRate(data.heartRate); // Update Heart Rate
//         } else {
//           console.error("set functions not defined properly in context");
//         }
//       } else {
//         console.warn("Received unrecognized message format:", message);
//       }
//     };

//     socket.onclose = () => {
//       console.log("WebSocket connection closed.");
//     };

//     socket.onerror = (error) => {
//       console.error("WebSocket error:", error);
//     };
//   };

//   useEffect(() => {
//     const intervalId = setInterval(fetchSensorData, 2000);
//     return () => clearInterval(intervalId);
//   }, []);

//   return (
//     <div className="dashboard" style={{ backgroundColor: "#ffffff" }}>
//       <Header />
//       <h1 style={{ textAlign: "center", color: "#000000" }}></h1> {/* Title */}
//       <div className="gaps">
//         <RealTimeVisualizer title="ECG Levels:" chartData={ecg} />
//         <RealTimeVisualizer title="Body Posture:" chartData={bodyPosture} />
//         <RealTimeVisualizer title="Heart Rate:" chartData={heartRate} />
//         <RealTimeVisualizer title="Temperature:" chartData={temperature} />
//       </div>
//       <div className="metrics-prediction">
//         <Metrics />
//         <Prediction />
//       </div>
//     </div>
//   );
// }

// export default Dashboard;


import { useEffect, useState } from "react";
import Header from "../Component/Header";
import RealTimeVisualizer from "../Component/RealTimeVisualizer";
import { useAppState } from "../GlobalContext/AppContext";

function Dashboard() {
  // Get context values with fallback to local state if context isn't working
  const contextValues = useAppState();
  
  // Create local state as fallback

  const [localTemperature, setLocalTemperature] = useState([]);
  const [localHeartRate, setLocalHeartRate] = useState([]);
  
  // Use context values if available, otherwise use local state
  
  const temperature = contextValues?.temperature || localTemperature;
  const heartRate = contextValues?.heartRate || localHeartRate;
  
  // Use setter functions from context if available, otherwise use local setters
 
  const setTemperature = contextValues?.setTemperature || setLocalTemperature;
  const setHeartRate = contextValues?.setHeartRate || setLocalHeartRate;

  useEffect(() => {
    const socket = new WebSocket("ws://127.0.0.1:5000");
    
    socket.onopen = () => {
      console.log("✅ WebSocket connection established.");
    };
    
    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        console.log("📥 Received WebSocket message:", message);
        
        if (message.SensorData) {
          const sensorData = message.SensorData;
          
          // Update state with sensor data

          
          if (typeof sensorData.temperature !== 'undefined') {
            setTemperature(prevTemp => [...(Array.isArray(prevTemp) ? prevTemp : []), sensorData.temperature]);
          }
          
          if (typeof sensorData.heartRate !== 'undefined') {
            setHeartRate(prevHR => [...(Array.isArray(prevHR) ? prevHR : []), sensorData.heartRate]);
          }
        } else {
          console.warn("⚠️ Unexpected message format:", message);
        }
      } catch (error) {
        console.error("❌ Error parsing WebSocket message:", error);
      }
    };
    
    socket.onclose = () => {
      console.log("WebSocket connection closed.");
    };
    
    socket.onerror = (error) => {
      console.error("❌ WebSocket error:", error);
    };
    
    return () => {
      socket.close();
      console.log("WebSocket cleanup executed.");
    };
  }, []); // No dependencies needed as we're using function closures

  return (
    <div className="dashboard" style={{ backgroundColor: "#ffffff" }}>
      <Header />
      <h1 style={{ textAlign: "center", color: "#000000" }}>Patient Monitoring</h1>
      <div className="gaps">
        <RealTimeVisualizer title="Atmospheric Temperature:" chartData={heartRate} />
        <RealTimeVisualizer title="Body Temperature:" chartData={temperature} />
        <RealTimeVisualizer title="Pressure:" chartData={temperature} />
        
      </div>
    </div>
  );
}

export default Dashboard;