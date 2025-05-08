import json
import time
import threading
import boto3
from websocket import WebSocketApp, create_connection


# Initialize DynamoDB resource
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("new_table")

# Initialize default data
data = {
    "bmp_temp": 0.0,
    "probe_temp": 0.0,
    "pressure": 0.0,
}

# Decimal encoder for DynamoDB float values
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

def normalize(value, min_val, max_val):
    """Normalize a value to a 0-1 scale."""
    return (value - min_val) / (max_val - min_val) if max_val != min_val else 0


def preprocess(data):
    """Preprocess sensor data by normalizing values."""
    processed_data = {
        "bmp_temp": normalize(data["bmp_temp"], 20, 40),  # Normal BMP temp range (just an example)
        "probe_temp": normalize(data["probe_temp"], 20, 40),  # Normal Probe temp range
        "pressure": normalize(data["pressure"], 900, 1050),  # Normal pressure range (example)
    }
    return processed_data


def analyze(processed_data):
    """Analyze processed data to determine risk level."""
    risk_level = 0
    if processed_data["bmp_temp"] > 0.8:
        risk_level += 1
    if processed_data["probe_temp"] > 0.8:
        risk_level += 1
    if processed_data["pressure"] < 0.2:  # Example threshold for low pressure
        risk_level += 1

    if risk_level >= 2:
        print("⚠️ Warning: Potential issue detected!")
    else:
        print("✅ Conditions appear stable.")

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


def fetch_data_from_dynamodb():
    """Fetch the latest payload from DynamoDB."""
    try:
        print("Fetching data from DynamoDB...")
        response = table.scan()
        items = response.get("Items", [])

        if not items:
            print("No items found in DynamoDB.")
            return {}

        # Get the latest item based on timestamp (assuming there's a 'timestamp' field)
        latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))

        payload = latest_item.get("payload", {})

        if not isinstance(payload, dict):
            print("Invalid payload format.")
            return {}

        # Extracting data from payload (bmp_temp, probe_temp, pressure)
        data = {
            "bmp_temp": float(payload.get("bmp_temp", {}).get("N", 0.0)),
            "probe_temp": float(payload.get("probe_temp", {}).get("N", 0.0)),
            "pressure": float(payload.get("pressure", {}).get("N", 0.0)),
        }

        return data
    except Exception as e:
        print(f"❌ Error fetching data from DynamoDB: {e}")
        return {}


def on_message(ws, message):
    """Handles incoming WebSocket messages."""
    global data
    try:
        # Fetch the latest sensor data from DynamoDB
        new_data = fetch_data_from_dynamodb()

        if new_data:
            # Update state variables with data from DynamoDB
            data["bmp_temp"] = new_data.get("bmp_temp", 0.0)
            data["probe_temp"] = new_data.get("probe_temp", 0.0)
            data["pressure"] = new_data.get("pressure", 0.0)

            print(f"📡 Updated Data from DynamoDB: {json.dumps(data, indent=4)}")

            # Process and analyze the data
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
