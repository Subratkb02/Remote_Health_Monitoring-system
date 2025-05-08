import asyncio
import websockets
import json

async def receive_data():
    """Connect to WebSocket server and receive data."""
    uri = "ws://127.0.0.1:5000"  # Change the server URI if needed
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server.")

            # Continuously receive data from server
            while True:
                response = await websocket.recv()
                data = json.loads(response)  # Parse the incoming data
                print("Received Data:", json.dumps(data, indent=4))

    except websockets.exceptions.ConnectionClosed:
        print("Connection to server closed.")
    except Exception as e:
        print(f"Error: {e}")

# Run the client
asyncio.get_event_loop().run_until_complete(receive_data())
