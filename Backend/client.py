# import websocket
# import json
# import threading
# import time
#
# # Initialize default data
# data = {
#     "heartRate": 0,
#     "temperature": 0.0,
#     "ecg": 0
# }
#
#
# def normalize(value, min_val, max_val):
#     return (value - min_val) / (max_val - min_val) if max_val != min_val else 0
#
#
# def preprocess(data):
#     processed_data = {
#         "heartRate": normalize(data["heartRate"], 60, 120),  # Normal HR range
#         "temperature": normalize(data["temperature"], 35, 40),  # Normal body temp range
#         "ecg": normalize(data["ecg"], 0, 4095)  # Adjust range as needed
#     }
#     return processed_data
#
#
# def analyze(processed_data):
#     risk_level = 0
#     if processed_data["heartRate"] > 0.8:
#         risk_level += 1
#     if processed_data["temperature"] > 0.8:
#         risk_level += 1
#     if processed_data["ecg"] > 0.7:
#         risk_level += 1
#
#     if risk_level >= 2:
#         print("Warning: High likelihood of a cardiovascular issue!")
#     else:
#         print("Cardiovascular condition appears stable.")
#
#     return risk_level
#
#
# def send_data_to_endpoint(processed_data, risk_level):
#     endpoint = "ws://0.0.0.0:5500"
#     ws = websocket.create_connection(endpoint)
#     message = {
#         "processed_data": processed_data,
#         "risk_level": risk_level,
#         "timestamp": time.time(),
#     }
#     ws.send(json.dumps(message))
#     ws.close()
#
#
# def on_message(ws, message):
#     global data
#     new_data = json.loads(message)
#
#     sensor_data = new_data.get("SensorData", {})
#
#     data["heartRate"] = int(sensor_data.get("heartRate", 0))
#     data["temperature"] = float(sensor_data.get("temperature", 0.0))
#     data["ecg"] = int(sensor_data.get("ecg", 0))
#
#     if data["temperature"]:
#         processed_data = preprocess(data)
#         risk_level = analyze(processed_data)
#         send_data_to_endpoint(processed_data, risk_level)
#
#
# def on_error(ws, error):
#     print(f"Error: {error}")
#
#
# def on_close(ws, close_status_code, close_msg):
#     print("Connection closed")
#
#
# def on_open(ws):
#     print("Connection opened")
#
#
# ws = websocket.WebSocketApp(
#     "ws://127.0.0.1:5050",
#     on_message=on_message,
#     on_error=on_error,
#     on_close=on_close,
#     on_open=on_open,
# )
#
#
# def run_websocket():
#     ws.run_forever()
#
#
# # Run WebSocket client in a separate thread
# websocket_thread = threading.Thread(target=run_websocket)
# websocket_thread.start()
#
# try:
#     while True:
#         time.sleep(1)
# except KeyboardInterrupt:
#     ws.close()
#     websocket_thread.join()
import json
import time
import threading
from websocket import WebSocketApp, create_connection  # Importing correctly

# Initialize default data
data = {
    "heartRate": 0,
    "temperature": 0.0,
}


def normalize(value, min_val, max_val):
    """Normalize a value to a 0-1 scale."""
    return (value - min_val) / (max_val - min_val) if max_val != min_val else 0


def preprocess(data):
    """Preprocess sensor data by normalizing values."""
    processed_data = {
        "heartRate": normalize(data["heartRate"], 60, 120),  # Normal HR range
        "temperature": normalize(data["temperature"], 35, 40),  # Normal body temp range
    }
    return processed_data


def analyze(processed_data):
    """Analyze processed data to determine risk level."""
    risk_level = 0
    if processed_data["heartRate"] > 0.8:
        risk_level += 1
    if processed_data["temperature"] > 0.8:
        risk_level += 1

    if risk_level >= 2:
        print("⚠️ Warning: High likelihood of a cardiovascular issue!")
    else:
        print("✅ Cardiovascular condition appears stable.")

    return risk_level


def send_data_to_endpoint(processed_data, risk_level):
    """Send processed data to another WebSocket server (if needed)."""
    try:
        endpoint = "ws://127.0.0.1:5173"
        ws = create_connection(endpoint)
        message = {
            "processed_data": processed_data,
            "risk_level": risk_level,
            "timestamp": time.time(),
        }
        ws.send(json.dumps(message))
        ws.close()
        print("📤 Data sent to secondary endpoint")
    except Exception as e:
        print(f"❌ Error sending data: {e}")


def on_message(ws, message):
    """Handles incoming WebSocket messages."""
    global data
    try:
        new_data = json.loads(message)
        sensor_data = new_data.get("SensorData", {})

        # Update state variables
        data["heartRate"] = int(sensor_data.get("heartRate", 0))
        data["temperature"] = float(sensor_data.get("temperature", 0.0))

        print(f"📡 Received Data: {json.dumps(data, indent=4)}")

        # Process and analyze data
        processed_data = preprocess(data)
        risk_level = analyze(processed_data)

        # Send processed data to another endpoint if needed
        send_data_to_endpoint(processed_data, risk_level)

    except Exception as e:
        print(f"❌ Error processing message: {e}")


def on_error(ws, error):
    """Handles WebSocket errors."""
    print(f"❌ WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Handles WebSocket disconnection."""
    print("🔌 Connection closed. Reconnecting in 3 seconds...")
    time.sleep(3)
    start_websocket()  # Auto-reconnect


def on_open(ws):
    """Handles WebSocket connection opening."""
    print("✅ Connection opened to WebSocket server")


def start_websocket():
    """Initialize WebSocket connection."""
    ws = WebSocketApp(
        "ws://127.0.0.1:5050",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever()


# Run WebSocket client in a separate thread
websocket_thread = threading.Thread(target=start_websocket)
websocket_thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Stopping WebSocket client...")
    websocket_thread.join()
