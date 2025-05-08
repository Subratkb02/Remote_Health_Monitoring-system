import { createContext, useContext, useState } from "react";
import PropTypes from "prop-types";

const AppContext = createContext();

const AppFieldsProvider = ({ children }) => {
  const [bloodSpo2, setSpo2] = useState([]);
  const [bioImpendence, setBioImpedence] = useState([]);
  const [pulseRate, setPulseRate] = useState([]);
  const [bodyTemperature, setBodyTemp] = useState([]);
  const [beatsAvg, setBeatsAvg] = useState([]);

  // ✅ Dynamically determine WebSocket URL based on environment
  const DATA_URL = import.meta.env.DEV
    ? "ws://localhost:5000"
    : "ws://54.83.118.12:5000"; // Use your domain or public IP here

  return (
    <AppContext.Provider
      value={{
        bloodSpo2,
        setSpo2,
        bioImpendence,
        setBioImpedence,
        pulseRate,
        setPulseRate,
        bodyTemperature,
        setBodyTemp,
        DATA_URL,
        beatsAvg,
        setBeatsAvg,
      }}
    >
      {children}
    </AppContext.Provider>
  );
};

export const useAppState = () => {
  return useContext(AppContext);
};

AppFieldsProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

export default AppFieldsProvider;
