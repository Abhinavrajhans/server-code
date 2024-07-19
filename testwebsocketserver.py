from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocketDisconnect
import redis
import json
import asyncio
import datetime
import direct_redis
import pandas as pd

app = FastAPI()

# Allow all origins in CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
r = direct_redis.DirectRedis()


# List of channels to subscribe to
channels = ['client_dashboard_data', 'basket_dashboard_data', 'connection_dashboard_data', 'strategy_mtm_chart_data']

async def handle_websocket_message(websocket: WebSocket, message: str):
    try:
        data = json.loads(message)
        if data['type'] == 'request_historical_data':
            try:
                earliest_timestamp = int(data['earliestTimestamp']) / 1000
                earliest_timestamp = datetime.datetime.fromtimestamp(earliest_timestamp)
            except TypeError as e:
                print(f"Error processing earliestTimestamp: {e}")
                print(f"earliestTimestamp value: {data.get('earliestTimestamp')}")
                return
            
            historical_data = get_historical_data(earliest_timestamp)
            await websocket.send_json({
                'channel': 'historical_data',
                'data': historical_data
            })
    except json.JSONDecodeError as e:
        print(f"Invalid JSON received: {e}")
        print(f"Raw message: {message}")
    except KeyError as e:
        print(f"Missing key in message: {e}")
    except Exception as e:
        print(f"Error handling message: {e}")
        print(f"Raw message: {message}")


def get_string_for_strategy_mtm_chart_data(str, lessTime):
    data = r.hgetall('live.mtm_' + str)
    df = pd.DataFrame(list(data.items()), columns=['Timestamp', 'Value'])
    
    # Convert Timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Set index and sort by Timestamp
    df.set_index('Timestamp', inplace=True)
    df.sort_values(by='Timestamp', inplace=True)
    
    # Filter for today's date
    today = datetime.datetime.now().date()
    df_today = df[df.index.date == today]
    
    # Filter for timestamps less than lessTime
    lessTime = pd.to_datetime(lessTime).time()
    df_today = df_today[df_today.index.time < lessTime]
    
    # Convert the filtered DataFrame to a dictionary with only key-value pairs
    df_today_dict = {timestamp.strftime('%Y-%m-%d %H:%M:%S'): value for timestamp, value in df_today['Value'].items()}
    
    # Display the dictionary
    return df_today_dict

# Example usage
# result = get_string_for_strategy_mtm_chart_data('your_strategy', '15:00:00')
# print(result)



def get_historical_data(earliest_timestamp):
    live_weights = r.hgetall('live_weights')
    keys_in_non_directional = list(live_weights['non_directional'].keys())
    keys_in_directional = list(live_weights['directional'].keys())
    # keys_in_nikbuy = list(live_weights['nikbuy'].keys())

    key_value_pairs_non_directional = [{key: get_string_for_strategy_mtm_chart_data(key,earliest_timestamp)} for key in keys_in_non_directional ]
    key_value_pairs_directional = [{key: get_string_for_strategy_mtm_chart_data(key,earliest_timestamp)} for key in keys_in_directional ]
    # key_value_pairs_nikbuy = [{key: get_last_value_for_strategy_mtm_chart_data(key)} for key in keys_in_nikbuy ]


    return {
        'non_directional': key_value_pairs_non_directional,
        'directional': key_value_pairs_directional,
        # 'nikbuy': key_value_pairs_nikbuy,
    }



@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connection opened")
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe(*channels)
    
    try:
        while True:
            try:
                client_message = await asyncio.wait_for(websocket.receive_text(), timeout=0.01)
                if client_message.strip():  # Check if message is not empty
                    print(f"Received message: {client_message}")  # Log the received message
                    await handle_websocket_message(websocket, client_message)
                else:
                    print("Received empty message from client")
            except asyncio.TimeoutError:
                pass  # No message received from client, continue to check Redis
            except json.JSONDecodeError as e:
                print(f"Invalid JSON received: {e}")
            except Exception as e:
                print(f"Error receiving message: {e}")
                print(f"Raw message: {client_message}")  # Log the raw message in case of error

            # ... rest of the function remains the same ...

    except WebSocketDisconnect:
        print("WebSocket disconnected")
    except Exception as e:
        print(f"Unexpected error in WebSocket connection: {e}")
    finally:
        pubsub.unsubscribe()
        print("WebSocket connection closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
