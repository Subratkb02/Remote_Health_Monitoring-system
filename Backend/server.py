import asyncio
import websockets
import json
import boto3
import decimal
from dotenv import load_dotenv
from pathlib import Path
import os

# ✅ Load AWS credentials from .env
load_dotenv()
# print("AWS_ACCESS_KEY_ID =", os.getenv("AWS_ACCESS_KEY_ID"))
# print("AWS_SECRET_ACCESS_KEY =", os.getenv("AWS_SECRET_ACCESS_KEY"))

# load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# ✅ Initialize DynamoDB with credentials
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-1",
    aws_access_key_id="key id",
    aws_secret_access_key="secret access key"
)

table = dynamodb.Table("new_table")
last_processed_payload = None

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

async def fetch_data_from_dynamodb():
    global last_processed_payload
    try:
        response = table.scan()
        items = response.get("Items", [])
        if not items:
            return {}
        latest_item = max(items, key=lambda x: int(x.get("timestamp", 0)))
        payload = latest_item.get("payload", {})
        if not isinstance(payload, dict):
            return {}
        if payload == last_processed_payload:
            return {}
        last_processed_payload = payload.copy()
        return {
            "SensorData": {
                "bmp_temp": float(payload.get("bmp_temp", 0.0)),
                "probe_temp": float(payload.get("probe_temp", 0.0)),
                "pressure": float(payload.get("pressure", 0.0))
            }
        }
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}

async def send_data_from_dynamodb(websocket):
    print("Client connected.")
    try:
        while True:
            data = await fetch_data_from_dynamodb()
            if data:
                await websocket.send(json.dumps(data))
            await asyncio.sleep(1)
    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected.")
    except Exception as e:
        print(f"Send error: {e}")

async def main():
    async with websockets.serve(send_data_from_dynamodb, "0.0.0.0", 5000):
        print("✅ WebSocket server running on ws://0.0.0.0:5000")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
