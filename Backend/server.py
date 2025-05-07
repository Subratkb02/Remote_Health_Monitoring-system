import asyncio
import websockets
import json
import boto3
import decimal
import logging
from boto3.dynamodb.conditions import Key
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS Configuration - Set your AWS credentials
# Method 1: Directly in code (not recommended for production, but useful for quick testing)
# Replace these with your actual AWS credentials
AWS_ACCESS_KEY_ID = "ACCESS_KEY"
AWS_SECRET_ACCESS_KEY = "SECRET_KEY"
AWS_REGION = "us-east-1"  # Replace with your AWS region

# Initialize DynamoDB resource with credentials
# Comment this section if using environment variables or AWS credential file
boto3_session = boto3.Session(
    aws_access_key_id="ACCESS_KEY",
    aws_secret_access_key="SECRET_KEY",
    region_name="us-east-1"
)
dynamodb = boto3_session.resource("dynamodb")

# Alternatively, if using environment variables or AWS credential file
# dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

# Your DynamoDB table name
TABLE_NAME = "project1_db"
table = dynamodb.Table(TABLE_NAME)

# Connected clients set
connected_clients = set()

# Custom JSON encoder to handle Decimal objects from DynamoDB
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)


# Fetch the latest data from DynamoDB
async def fetch_latest_data():
    try:
        # Update these with your actual table's partition key and sort key
        # This is a critical part - make sure these match your table structure
        partition_key = "timestamp"  # Replace with your partition key name
        partition_value = "String"  # Replace with your partition key value
        
        # Print the query we're about to run for debugging
        logger.info(f"Querying DynamoDB table {TABLE_NAME} for {partition_key}={partition_value}")
        
        response = table.query(
            KeyConditionExpression=Key(partition_key).eq(partition_value),
            ScanIndexForward=False,  # Get latest data first
            Limit=1  # Only fetch the most recent record
        )

        items = response.get("Items", [])

        if not items:
            logger.warning("No items found in DynamoDB query response")
            # Try a scan instead to see what's in the table
            scan_response = table.scan(Limit=1)
            scan_items = scan_response.get("Items", [])
            if scan_items:
                logger.info(f"Found item with scan: {json.dumps(scan_items[0], cls=DecimalEncoder)}")
            else:
                logger.warning("No items found in table with scan")

        # Initialize default data structure
        data = {
            "SensorData": {
                "heartRate": 0,
                "temperature": 0.0,
            }
        }

        if items:
            latest_item = items[0]  # Get the latest item
            logger.info(f"Raw item from DynamoDB: {json.dumps(latest_item, cls=DecimalEncoder)}")
            
            # Adjust this based on your actual data structure
            # This is just an example - you'll need to modify based on how your data is stored
            data["SensorData"]["heartRate"] = int(latest_item.get("heartRate", 0))
            data["SensorData"]["temperature"] = float(latest_item.get("temperature", 0.0))
            
            # Add timestamp if available
            if "timestamp" in latest_item:
                data["SensorData"]["timestamp"] = latest_item["timestamp"]

        logger.info(f"Processed data: {json.dumps(data, cls=DecimalEncoder)}")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from DynamoDB: {e}")
        # Print more details about the exception
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}


# Handle incoming client messages
async def handle_client(websocket):
    try:
        while True:
            message = await websocket.recv()
            received_data = json.loads(message)
            logger.info(f"Received from client: {json.dumps(received_data)}")
            
    except websockets.exceptions.ConnectionClosed:
        logger.info("Client disconnected.")
    except Exception as e:
        logger.error(f"Error receiving data from client: {e}")


# This is the primary handler function that adapts to different WebSockets library versions
async def handler_function(*args):
    # Extract websocket from args
    websocket = args[0]
    
    logger.info(f"Client connected")
    
    # Add to connected clients set
    connected_clients.add(websocket)
    
    # Start task to handle incoming messages from this client
    receive_task = asyncio.create_task(handle_client(websocket))

    try:
        while True:
            data = await fetch_latest_data()

            if data:
                # Convert to JSON using the DecimalEncoder
                json_data = json.dumps(data, cls=DecimalEncoder)
                await websocket.send(json_data)
                logger.info(f"Sent data to client")
            else:
                logger.warning("No data to send.")

            await asyncio.sleep(1)  # Fetch every second

    except websockets.exceptions.ConnectionClosed:
        logger.info("Client connection closed")
    except Exception as e:
        logger.error(f"Error sending data to client: {e}")
    finally:
        # Clean up
        connected_clients.discard(websocket)
        receive_task.cancel()
        try:
            await receive_task
        except asyncio.CancelledError:
            pass
        logger.info("Client handler finished")


# Start the WebSocket server
async def main():
    try:
        # Test DynamoDB connection first
        try:
            logger.info("Testing DynamoDB connection...")
            table.get_item(Key={"device_id": "test"})  # Replace with your actual partition key
            logger.info("DynamoDB connection successful!")
        except Exception as e:
            logger.error(f"DynamoDB connection test failed: {e}")
            logger.info("Continuing anyway...")
        
        # Start the WebSocket server
        server = await websockets.serve(handler_function, "127.0.0.1", 5173)
        logger.info("WebSocket server started on ws://127.0.0.1:5173")
        
        # Keep the server running
        await asyncio.Future()  # Run forever
    except Exception as e:
        logger.error(f"Server error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")