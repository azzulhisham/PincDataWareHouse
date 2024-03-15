import websockets
import asyncio
import socket
import select
import threading
import time
import math
import json
import datetime


from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *
from array import *


HEARTBEAT_INTERVAL = 30
connected_socket = {}

def check_ais_checksum(ais_msg):
    ais_str = ais_msg[1:ais_msg.index('*')]
    ais_chksum = ais_msg[ais_msg.index('*')+1:]
    
    ais_byte_arr = [ord(n) for n in ais_str]
    xor_sum = 0
    
    for n in ais_byte_arr:
        xor_sum ^= n 
    
    return True if xor_sum == int('0x' + ais_chksum, 16) else False


def ais_binaryString(ais_array):
    ais_armoring = "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVW`abcdefghijklmnopqrstuvw"
    binaryString = ''

    for i in ais_array:
        ais = i.split(',')

        for n in ais[5]:
            idx = ais_armoring.index(n)
            binaryString +=bin(idx)[2:].zfill(6)

    return binaryString


def ais_parser(binaryString):
    msgType = Nmea.binary_parser(binaryString, 0, 6, False) 

    data = {
        'messageType' : msgType,
        'messageTypeDesc' : MessageType(msgType).name.replace('_', " "),
        'repeat': Nmea.binary_parser(binaryString, 6, 2, False),
        'mmsi': Nmea.binary_parser(binaryString, 8, 30, False)  
    }

    if msgType == 1 or msgType == 2 or msgType == 3:
        ais_position = Nmea.ais_position_parser(binaryString)
        data.update(ais_position)

    if msgType == 4:
        ais_baseStation = Nmea.ais_baseStation_parser(binaryString)
        data.update(ais_baseStation)  

    if msgType == 5:
        ais_static = Nmea.ais_static_parser(binaryString)
        data.update(ais_static)   

    if msgType == 6:
        ais_aton = Nmea.ais_aton_parser(binaryString)
        data.update(ais_aton)  
        
    if msgType == 8:
        ais_binaryBroadcast = Nmea.ais_binaryBroadcast_parser(binaryString)
        data.update(ais_binaryBroadcast)     

    if msgType == 9: 
        ais_aircraftPosition = Nmea.ais_aircraftPosition_parser(binaryString)
        data.update(ais_aircraftPosition) 

    if msgType == 12: 
        ais_addressSafety = Nmea.ais_addressSafety_parser(binaryString)
        data.update(ais_addressSafety) 

    if msgType == 14: 
        ais_SafetyBroadcast = Nmea.ais_SafetyBroadcast_parser(binaryString)
        data.update(ais_SafetyBroadcast) 

    if msgType == 15: 
        ais_interrogation = Nmea.ais_interrogation_parser(binaryString)
        data.update(ais_interrogation) 

    if msgType == 16: 
        ais_AssignmentMode = Nmea.ais_AssignmentMode_parser(binaryString)
        data.update(ais_AssignmentMode)     

    if msgType == 17: 
        ais_DGNSS = Nmea.ais_DGNSS_parser(binaryString)
        data.update(ais_DGNSS)    

    if msgType == 18: 
        ais_classB_position = Nmea.ais_classB_position_parser(binaryString)
        data.update(ais_classB_position)  

    if msgType == 19: 
        ais_classB_positionX = Nmea.ais_classB_positionX_parser(binaryString)
        data.update(ais_classB_positionX)  

    if msgType == 21: 
        ais_aid_nav = Nmea.ais_aid_navigation_parser(binaryString)
        data.update(ais_aid_nav)  

    if msgType == 24: 
        ais_static_report = Nmea.ais_static_report_parser(binaryString)
        data.update(ais_static_report) 

    if msgType == 27: 
        ais_long_range_broadcast = Nmea.ais_long_range_broadcast_parser(binaryString)
        data.update(ais_long_range_broadcast) 


    return data


def ais_decode(ais_array):
    package_type = ''
    package_ch = ''
    package_ID = ''
    prev_package = ''

    if len(ais_array) > 0 :
        for ais_msg in ais_array:
            ais = ais_msg.split(',')
            package_type = ais[0]
            package_ID = int(ais[3]) if ais[3] else 0
            package_ch = ais[4]

            total_package = int(ais[1])
            package_no = int(ais[2])
            package_id = int(ais[3]) if ais[3] else 0

            # validate ais package number
            if total_package > 1 :
                if total_package != len(ais_array) :
                    print('[ERROR ::] Invalid total package of AIS message.')
                    return None
            

            # validate checksum
            if not check_ais_checksum(ais_msg=ais_msg):
                print('[ERROR ::] Invalid AIS. Checksum error.')
                return None

            # validate previous ais package
            if prev_package :
                p_ais = prev_package.split(',')
                p_total_package = int(p_ais[1])
                p_package_no = int(p_ais[2])
                p_package_id = int(p_ais[3]) if p_ais[3] else 0

                if total_package != p_total_package or p_package_no != package_no-1 or p_package_id != package_id:
                    print('[ERROR ::] Invalid AIS. Package not in sequence.')
                    return None                 

            prev_package = ais_msg


        package_data = {
            'packageType': package_type,
            'packageID': package_ID,
            'packageCh': package_ch
        }

        parsed_data = ais_parser(ais_binaryString(ais_array))
        package_data.update(parsed_data)

        # todo :: here will be the next data processing
        # print(json.dumps(package_data, indent=4))
        return package_data

    else:
        print('[ERROR ::] No package found.')    



def ais_ingress():
    host = ['MYKUL-MBP-02.local']
    port = [38383]
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
                        if data.strip()[0:1] == '!':
                            ais_sentence = data.strip()
                            print(ais_sentence)

                            try:
                                result = ais_decode(ais_sentence.split('|'))
                                
                                if result != None:
                                    print(result)
                                    if (result["messageType"] == 1 or result["messageType"] == 2 or result["messageType"] == 3) and result['sog'] != 0.0:
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


def main():
    # Get local machine name
    host = socket.gethostname()

    # Reserve a port for your service
    port = 58383

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

