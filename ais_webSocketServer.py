import websockets
import asyncio
import socket
import select
import threading
import time
import math
import json
import datetime
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *
from array import *


HEARTBEAT_INTERVAL = 30
connected_socket = {}

def ais_ingress():
    host = ['MYKUL-MBP-02.local']
    port = [58383]
    ais_key_str = ['014e4d45415f4d444d004e4d45415f4d444d00']

    timeout = 30
    targetHost = 0


    while True:
        aiskey = array('b', [])
        keyLenCnt = 0
        keyHexVal = ''
        
        for n in range(int(len(ais_key_str[targetHost]))):
            keyHexVal += ais_key_str[targetHost][n:n+1]
            keyLenCnt += 1

            if keyLenCnt % 2 == 0:
                aiskey.append(int(keyHexVal, 16))
                keyHexVal = ''


        try:
            data = ''
            defaultTime = datetime.datetime(1900, 1, 1, 00, 00, 00)
            now = datetime.datetime(1900, 1, 1, 00, 00, 00)

            print(f'attempt to server {host[targetHost]} via port {port[targetHost]}......')

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host[targetHost], port[targetHost]))
            s.settimeout(timeout)
            s.setblocking(0)        #set to unblocking socket operation
            s.send(aiskey)

            print('connected...')
            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            multi_package_ais = []

            while True:
                try:       
                    recvData = s.recv(1)
                    
                    if len(recvData) == 0:
                        raise Exception
                    else:
                        now = defaultTime

                    data += recvData.decode()

                    if recvData.decode() == '\n':
                        ais_json = data.strip()
                        print(ais_json)

                        try:
                            result = json.loads(ais_json)
                            
                            if result != None:
                                print(result)

                                if ((result["messageType"] == 1 or result["messageType"] == 2 or result["messageType"] == 3) and result['sog'] != 0.0) or result["messageType"] == 6 or result["messageType"] == 21 :
                                    if result["messageType"] == 21:
                                        aton = getAton(result["mmsi"])

                                        if aton.shape[0] > 0 :
                                            aton_info = {
                                                'atonname': aton.iloc[0]['name'],
                                                'region': aton.iloc[0]['region'],
                                                'type': aton.iloc[0]['type']
                                            }

                                            result.update(aton_info)
                                        else:
                                            continue
                                    
                                    
                                    for q in connected_socket.values():
                                        # if q.full == False:
                                        q.put_nowait(result)
                                        
                            else:
                                print("None")
                        except:
                            print("ais_decode---------------------------------------error")

                        data = ''

                    if s.fileno() == -1:
                        break
                except:
                    if now == defaultTime:
                        now = datetime.datetime.now()

                    dt = datetime.datetime.now() - now
                    
                    if dt.total_seconds() > timeout:
                        break

                    continue
            
            s.close()
            s.detach()
            print('timeout - process exited!')

            time.sleep(1)

        except:
            s.detach()

            print('network reconnection....')
            time.sleep(3)

            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            continue



async def handler(websocket, path):
    # Start a task for sending ping messages periodically
    ping_task = asyncio.create_task(send_ping(websocket))

    q = asyncio.Queue(0)
    data_task = asyncio.create_task(send_data(websocket, q))
    connected_socket[websocket] = q
    
    print(f"[PATH]:: {path}")

    try:
        # Receive messages from the client
        async for message in websocket:
            print(f"[MEESSAGE]:: {message}")


    finally:
        # Cancel the ping task when the connection is closed
        ping_task.cancel()
        data_task.cancel()
        del connected_socket[websocket]


async def send_data(websocket, queue):
    while True:
        await asyncio.sleep(0.01)

        if queue.empty() == False:
            msg = await queue.get()
            queue.task_done()
            await websocket.send(json.dumps(msg))


async def send_ping(websocket):
    # Send ping messages periodically
    while True:
        # Wait for the heartbeat interval
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        # Send a ping message and wait for a pong response
        await websocket.ping()
        print("Send a ping message!")


def loaddata():
    global df
    df = pd.read_csv('/Users/zultan/sources/python/socket/docs/atonlist.csv') 


def getAton(mmsi):
    aton = df[df['mmsi'] == mmsi]
    return aton


def main():
    loaddata()

    # Get local machine name
    host = socket.gethostname()

    # Reserve a port for your service
    port = 18383

    print(f"Server {host} is ruuning on port {port} -- v1.0")

    # Create a WebSocket server using the handler function
    server = websockets.serve(handler, host, port)

    # Get the event loop object
    global loop 
    loop = asyncio.get_event_loop()

    
    thread_one = threading.Thread(target=ais_ingress)
    thread_one.start()

    # Run the server using the asyncio event loop
    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == '__main__':
    main()

