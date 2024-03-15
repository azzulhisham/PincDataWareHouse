import socket
import time
import math
import json
import datetime
import clickhouse_connect
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
from collections import deque

from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *
from array import *

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def data_transform(payload):
    data = {
        'ts': payload['ts'], 
        'packageType': payload['packageType'], 
        'packageID': payload['packageID'], 
        'packageCh': payload['packageCh'], 
        'messageType': payload['messageType'], 
        'messageTypeDesc': payload['messageTypeDesc'], 
        'repeat': payload['repeat'], 
        'mmsi': payload['mmsi'], 
        'seqno': payload['seqno'], 
        'dest_mmsi': payload['dest_mmsi'], 
        'retransmit': payload['retransmit'], 
        'dac': payload['dac'], 
        'fid': payload['fid'], 
        'volt_int': payload['volt_int'], 
        'volt_ex1': payload['volt_ex1'], 
        'volt_ex2': payload['volt_ex2'], 
        'off_pos': payload['off_pos'], 
        'ambient': payload['ambient'], 
        'racon': payload['racon'], 
        'light': payload['light'], 
        'health': payload['health'], 
        'beat': payload['beat'], 
        'alarm_active': 0, 
        'buoy_led_power': payload['lantern_batt'], 
        'buoy_low_vin': payload['lantern'], 
        'buoy_photocell': payload['hatch_door'], 
        'buoy_temp': 0, 
        'buoy_force_off': 0, 
        'buoy_islight': 0, 
        'buoy_errled_short': 0, 
        'buoy_errled_open': 0, 
        'buoy_errled_voltlow': 0, 
        'buoy_errled_vinlow': 0, 
        'buoy_errled_power': 0, 
        'buoy_adjmaxpower': 0, 
        'buoy_sensor_interrupt': 0, 
        'buoy_solarcharging': 0
    }    

    value_list = [value for value in data.values()]
    return value_list


def ais_ingress(client):
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
            
            logging.info(f'attempt to server {host[targetHost]} via port {port[targetHost]}......')

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host[targetHost], port[targetHost]))
            s.settimeout(timeout)
            s.setblocking(0)        #set to unblocking socket operation
            s.send(aiskey)

            logging.info('connected...')
            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            ais_123 = []
            ais_5 = []
            ais_21 = []
            ais_6_533 = []
            inMemoryRecords = 20

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
                        # print(ais_json)

                        try:
                            result = json.loads(ais_json)
                            
                            if result != None:
                                ts = datetime.datetime.now()
                                current_timestamp = int(ts.timestamp() * 1000)
                                payload = {'ts': current_timestamp}
                                payload.update(result)
                                logging.info(payload)

                                if (result["messageType"] == 1 or result["messageType"] == 2 or result["messageType"] == 3):
                                    value_list = [value for value in payload.values()]
                                    key_list = [value for value in payload.keys()]
                                    ais_123.append(value_list)

                                    if len(ais_123) >= inMemoryRecords:
                                        client.insert('pnav.ais_position', ais_123, column_names=key_list)
                                        ais_123.clear()

                                if result["messageType"] == 5:
                                    value_list = [value for value in payload.values()]
                                    key_list = [value for value in payload.keys()]
                                    ais_5.append(value_list)

                                    if len(ais_5) >= inMemoryRecords:
                                        client.insert('pnav.ais_static', ais_5, column_names=key_list)
                                        ais_5.clear()

                                if result["messageType"] == 21:
                                    value_list = [value for value in payload.values()]
                                    key_list = [value for value in payload.keys()]
                                    ais_21.append(value_list)

                                    if len(ais_21) >= inMemoryRecords:
                                        client.insert('pnav.ais_type21', ais_21, column_names=key_list)
                                        ais_21.clear()

                                if result["messageType"] == 6:
                                    value_list = [value for value in payload.values()]
                                    key_list = ['ts', 'packageType', 'packageID', 'packageCh', 'messageType', 'messageTypeDesc', 'repeat', 'mmsi', 'seqno', 'dest_mmsi', 'retransmit', 'dac', 'fid', 'volt_int', 'volt_ex1', 'volt_ex2', 'off_pos', 'ambient', 'racon', 'light', 'health', 'beat', 'alarm_active', 'buoy_led_power', 'buoy_low_vin', 'buoy_photocell', 'buoy_temp', 'buoy_force_off', 'buoy_islight', 'buoy_errled_short', 'buoy_errled_open', 'buoy_errled_voltlow', 'buoy_errled_vinlow', 'buoy_errled_power', 'buoy_adjmaxpower', 'buoy_sensor_interrupt', 'buoy_solarcharging']

                                    if result["dac"] == 533 and result["fid"] == 4:
                                        ais_6_533.append(value_list)

                                        if len(ais_6_533) >= inMemoryRecords:
                                            client.insert('pnav.ais_type6_533', ais_6_533, column_names=key_list)
                                            ais_6_533.clear()

                                    if result["dac"] == 533 and result["fid"] == 2:
                                        value_list.append(0)
                                        value_list.append(0)
                                        ais_6_533.append(value_list)

                                        if len(ais_6_533) >= inMemoryRecords:
                                            client.insert('pnav.ais_type6_533', ais_6_533, column_names=key_list)
                                            ais_6_533.clear()

                                    if result["dac"] == 533 and result["fid"] == 1:
                                        value_list = data_transform(payload)
                                        ais_6_533.append(value_list)

                                        if len(ais_6_533) >= inMemoryRecords:
                                            client.insert('pnav.ais_type6_533', ais_6_533, column_names=key_list)
                                            ais_6_533.clear()


                            else:
                                logging.info("None")
                        except:
                            logging.info("ais_decode---------------------------------------error")

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
            logging.info('timeout - process exited!')

            time.sleep(1)

        except:
            s.detach()

            logging.info('network reconnection....')
            time.sleep(3)

            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            continue


def main():
    client = clickhouse_connect.get_client(host='10.10.20.50', port=8123)
    ais_ingress(client)


if __name__ == '__main__':
    main()

