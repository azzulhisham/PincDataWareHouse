import websockets
import asyncio
import socket
import select
import threading
import time
import math
import json
import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
from collections import deque

from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *
from array import *


HEARTBEAT_INTERVAL = 30
connected_socket = {}

TSS_Northbound = deque()
TSS_Southbound = deque()


def indentify_vessel_within_zone(payload, zone):
    # create pandas dataframe 
    datalist = []
    data = payload
    datalist.append(data)
    df = pd.DataFrame.from_dict(datalist)

    # convert to geopandas dataframe
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )

    # define zone/polyzone to target
    # TSS-Northbound
    if zone == 1:
        zn = {"type": "FeatureCollection","features": [{"type": "Feature","properties": {},"geometry": {"type": "Polygon","coordinates": [[
            [100.81183434, 3.04546854],
            [100.78857422, 3.01186991],
            [100.92864990, 2.89426660],
            [100.99096298, 2.82448741],
            [101.16983414, 2.73207069],
            [101.22819901, 2.69983461],
            [101.43934250, 2.58854501],
            [101.44878387, 2.58357190],
            [101.61975861, 2.45152024],
            [101.68893814, 2.39800985],
            [101.83811188, 2.28069134],
            [101.87999725, 2.25015950],
            [101.99157715, 2.16267711],
            [102.07672119, 2.08977126],
            [102.25542068, 1.92902351],
            [102.41214752, 1.85404837],
            [102.73057938, 1.70168649],
            [102.80336380, 1.66428046],
            [102.99957275, 1.53077984],
            [103.17449570, 1.40859717],
            [103.20505142, 1.38783233],
            [103.39817047, 1.22960188],
            [103.47301483, 1.20351523],
            [103.49387169, 1.23831167],
            [103.42082977, 1.25427241],
            [103.33379745, 1.34085335],
            [103.28358650, 1.39109294],
            [103.28144073, 1.39606965],
            [103.24710846, 1.42936183],
            [103.17449570, 1.49182627],
            [103.09656143, 1.56063806],
            [103.00008774, 1.64386126],
            [102.74688721, 1.75624996],
            [102.62483597, 1.81115498],
            [102.49214172, 1.87069073],
            [102.27653503, 1.96711031],
            [102.10521698, 2.12185026],
            [102.10298538, 2.11996328],
            [102.01698303, 2.19372537],
            [101.99209213, 2.21671096],
            [101.94917679, 2.25015950],
            [101.94660187, 2.25067409],
            [101.79759979, 2.36748054],
            [101.71588898, 2.43162560],
            [101.71382904, 2.42973902],
            [101.64567947, 2.48290515],
            [101.64430618, 2.48513466],
            [101.52122498, 2.58065662],
            [101.52122498, 2.58271446],
            [101.46646500, 2.62558549],
            [101.42389297, 2.64856375],
            [101.40689850, 2.65559433],
            [101.24879837, 2.73961518],
            [101.24811172, 2.73858639],
            [101.18974686, 2.77236458],
            [100.99868774, 2.87540778],
            [100.99473953, 2.87832234],
            [100.81183434, 3.04546854]]]}}]}

    # TSS-Southbound
    if zone == 2:
        zn = {"type": "FeatureCollection","features": [{"type": "Feature","properties": {},"geometry": {"type": "Polygon","coordinates": [[
            [100.78385353, 3.00488435],
            [100.90719223, 2.90236723],
            [100.91860771, 2.89276648],
            [100.92882156, 2.88247987],
            [100.98993301, 2.81741496],
            [101.11541748, 2.74994592],
            [101.16545677, 2.72319728],
            [101.22510910, 2.69370451],
            [101.29875183, 2.65503703],
            [101.30235672, 2.65220765],
            [101.37248039, 2.61508222],
            [101.42766953, 2.58738747],
            [101.53521538, 2.50232741],
            [101.61194801, 2.44110132],
            [101.67975426, 2.38638993],
            [101.85819626, 2.24844421],
            [101.98282242, 2.15049779],
            [102.06418991, 2.07604743],
            [102.24666595, 1.91358267],
            [102.54690170, 1.77169216],
            [102.79975891, 1.65141124],
            [102.99991608, 1.52031221],
            [103.20505142, 1.38680267],
            [103.38752747, 1.22119240],
            [103.45636368, 1.17502558],
            [103.44451904, 1.15408722],
            [103.37757111, 1.19390430],
            [103.23337555, 1.32244764],
            [103.18239212, 1.36775376],
            [102.98103333, 1.49011024],
            [102.77812958, 1.61417585],
            [102.53213882, 1.72999792],
            [102.29764938, 1.84015102],
            [102.22297668, 1.87532311],
            [102.03826904, 2.04748431],
            [101.95724487, 2.11936288],
            [101.65477753, 2.35744689],
            [101.38732910, 2.56702330],
            [101.14528656, 2.68740291],
            [100.89706421, 2.81771500],
            [100.71613312, 2.91269651],
            [100.78385353, 3.00488435]
        ]]}}]}

    # convert geojson to geopandas dataframe        
    geozone_df = gpd.GeoDataFrame.from_features(zn, crs="EPSG:4326")

    # join the 2 dataframe
    vessel_in_zone = gpd.sjoin(gdf, geozone_df, how='inner', predicate='within')

    # return the result
    return vessel_in_zone.shape[0]


def ais_ingress():
    host = ['MYKUL-MBP-02.local']
    port = [58383]
    ais_key_str = ['014e4d45415f4d444d004e4d45415f4d444d00']

    timeout = 30
    targetHost = 0

    cnt_left_TSS_N = 0
    cnt_left_TSS_S = 0

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
                                # print(result)

                                if (result["messageType"] == 1 or result["messageType"] == 2 or result["messageType"] == 3):
                                    try:
                                        # TSS-Northbound
                                        is_within = indentify_vessel_within_zone(result, 1)

                                        if is_within:
                                            if result["mmsi"] not in TSS_Northbound:
                                                TSS_Northbound.append(result["mmsi"])
                                        else:
                                            if result["mmsi"] in TSS_Northbound:
                                                TSS_Northbound.remove(result["mmsi"])
                                                cnt_left_TSS_N += 1

                                        # TSS-Southbound
                                        is_within = indentify_vessel_within_zone(result, 2)

                                        if is_within:
                                            if result["mmsi"] not in TSS_Southbound:
                                                TSS_Southbound.append(result["mmsi"])
                                        else:
                                            if result["mmsi"] in TSS_Southbound:
                                                TSS_Southbound.remove(result["mmsi"])
                                                cnt_left_TSS_S += 1

                                        summary = {
                                            "cnt_in_TSS_N" : len(TSS_Northbound),
                                            "cnt_in_TSS_S" : len(TSS_Southbound),
                                            "cnt_left_TSS_N" : str(cnt_left_TSS_N),
                                            "cnt_left_TSS_S" : str(cnt_left_TSS_S)
                                        }

                                        print(summary)

                                        for q in connected_socket.values():
                                            q.put_nowait(summary)    


                                    except:
                                        print("Error in doing summary..............")


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


def main():
    # Get local machine name
    host = socket.gethostname()

    # Reserve a port for your service
    port = 28383

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

