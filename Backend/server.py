import asyncio
import websockets
import json
import boto3
import decimal

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("new_table")

# Store the last processed payload
last_processed_payload = None

# Decimal encoder for DynamoDB float values
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

async def fetch_data_from_dynamodb():
    """Fetch the latest payload from DynamoDB based on timestamp."""
    global last_processed_payload

    try:
        print("Fetching data from DynamoDB...")
        response = table.scan()  # You may want to use query instead of scan if you can filter data
        items = response.get("Items", [])

        if not items:
            print("No items found in DynamoDB.")
            return {}

        # Get the latest item based on 'timestamp' field
        latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))

        payload = latest_item.get("payload", {})

        # Validate payload format
        if not isinstance(payload, dict):
            print("Invalid payload format.")
            return {}

        # Check if payload is new
        if payload == last_processed_payload:
            print("No new payload detected.")
            return {}

        # Store a copy to track changes
        last_processed_payload = payload.copy()

        data = {
            "SensorData": {
                "bmp_temp": float(payload.get("bmp_temp", 0.0)),
                "probe_temp": float(payload.get("probe_temp", 0.0)),
                "pressure": float(payload.get("pressure", 0.0))
            }
        }

        print("New data fetched:", json.dumps(data, indent=4))
        return data

    except Exception as e:
        print(f"Error fetching data from DynamoDB: {e}")
        return {}

async def handle_client(websocket):
    """Handle client messages if needed."""
    print("New client connected")
    try:
        while True:
            message = await websocket.recv()
            received_data = json.loads(message)
            print("Received from client:", json.dumps(received_data, indent=4))
    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected.")
    except Exception as e:
        print(f"Error receiving from client: {e}")

async def send_data_from_dynamodb(websocket, path):
    """Send data to WebSocket clients."""
    receive_task = asyncio.create_task(handle_client(websocket))

    try:
        while True:
            data = await fetch_data_from_dynamodb()

            if data:
                print("Sending new data to client...")
                await websocket.send(json.dumps(data))
            else:
                print("No new data to send.")
                # Optionally, send the last known data if needed
                if last_processed_payload:
                    await websocket.send(json.dumps({"SensorData": last_processed_payload}))

            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error sending data to client: {e}")

    finally:
        receive_task.cancel()
        try:
            await receive_task
        except asyncio.CancelledError:
            pass
        print("Receive task cancelled.")

# Start WebSocket server
start_server = websockets.serve(send_data_from_dynamodb, "127.0.0.1", 5000)

print("WebSocket server starting on ws://127.0.0.1:5000")
asyncio.get_event_loop().run_until_complete(start_server)
print("WebSocket server is running.")
asyncio.get_event_loop().run_forever()
