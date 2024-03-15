import time
import math
import json
import datetime
import threading

import polars as pl
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from collections import deque
from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *
from array import *




TSS_Northbound = deque()
TSS_Southbound = deque()


def get_geozone(zone):
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

    # return the result
    return geozone_df


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


def main():
    startTime = time.time()

    global cnt_left_TSS_N
    global cnt_left_TSS_S

    filedate = '20230109'

    # read raw data from files
    print('---- Reading raw data ----')
    with open(f"/Users/zultan/Downloads/Datalog_archive_v70_{filedate}_000000_831") as f:
        raw_data = f.read()

    data = raw_data.split('\n')
    totalrow = len(data)
    rowcnt = 0

    aisdatalist = []
    aisstatic = []
    aisstatictag = ''
    aisstaticlist = []

    # process message type 1, 2, 3, 5 and 24
    print('---- Decoding data ----')
    for i in data:
        if i != '':
            ais = i[i.index('!'):]
            det = ais.split(',')

            totalPackage = det[1]
            packageId = det[2]
            msgType = det[5]
            msgType = msgType[0:1]

            if totalPackage == '2':
                if packageId == '1':
                    tagblock = i[:i.index('!')]
                    aisstatictag = tagblock

                aisstatic.append(ais)

                if int(totalPackage) == len(aisstatic):
                    tagblock = aisstatictag
                    tags = tagblock.split(',')
                    tagtime = tags[2]
                    tagtime = tagtime[2:tagtime.index('*')]
                    ais_datetime = datetime.datetime.utcfromtimestamp(int(tagtime))

                    result = ais_decode(aisstatic)
                    aisstatictag = ''
                    aisstatic.clear()
                    
                    if result['messageType'] == 5:
                        aisdata = {
                            'ts': ais_datetime, 
                            'messageType': result['messageType'], 
                            'mmsi': result['mmsi'], 
                            'imo': result['imo'] if "imo" in result else None, 
                            'callsign': result['callsign'] if "callsign" in result else None,
                            'shipName': result['shipName'] if "shipName" in result else None, 
                            'shipType': result['shipType'] if "shipType" in result else None,
                            'destination': result['destination'] if "destination" in result else None,
                            'eta_month': result['eta_month'] if "eta_month" in result else None,
                            'eta_day': result['eta_day'] if "eta_day" in result else None,
                            'eta_hour': result['eta_hour'] if "eta_hour" in result else None,
                            'eta_minute': result['eta_minute'] if "eta_minute" in result else None
                        }

                        aisstaticlist.append(aisdata) 
                else:
                    rowcnt += 1
                    continue

            if totalPackage == '1':
                if msgType == '1' or msgType == '2' or msgType == '3' :
                    tagblock = i[:i.index('!')]
                    tags = tagblock.split(',')
                    tagtime = tags[2]
                    tagtime = tagtime[2:tagtime.index('*')]
                    ais_datetime = datetime.datetime.utcfromtimestamp(int(tagtime))

                    result = ais_decode([ais])

                    aisdata = {
                        'ts': ais_datetime, 
                        'messageType': result['messageType'], 
                        'mmsi': result['mmsi'], 
                        'sog': result['sog'], 
                        'cog': result['cog'], 
                        'rot': result['rot'], 
                        'trueHeading': result['trueHeading'],
                        'latitude': result['latitude'],
                        'longitude': result['longitude'],
                    }

                    aisdatalist.append(aisdata)   

                elif msgType == 'H':
                    tagblock = i[:i.index('!')]
                    tags = tagblock.split(',')
                    tagtime = tags[2]
                    tagtime = tagtime[2:tagtime.index('*')]
                    ais_datetime = datetime.datetime.utcfromtimestamp(int(tagtime))

                    result = ais_decode([ais])
                    
                    aisdata = {
                        'ts': ais_datetime, 
                        'messageType': result['messageType'], 
                        'mmsi': result['mmsi'], 
                        'imo': result['imo'] if "imo" in result else None, 
                        'callsign': result['callsign'] if "callsign" in result else None,
                        'shipName': result['shipName'] if "shipName" in result else None, 
                        'shipType': result['shipType'] if "shipType" in result else None,
                        'destination': result['destination'] if "destination" in result else None,
                        'eta_month': result['eta_month'] if "eta_month" in result else None,
                        'eta_day': result['eta_day'] if "eta_day" in result else None,
                        'eta_hour': result['eta_hour'] if "eta_hour" in result else None,
                        'eta_minute': result['eta_minute'] if "eta_minute" in result else None
                    }

                    aisstaticlist.append(aisdata) 

        rowcnt += 1
        print(f'{rowcnt}/{totalrow}')


    print('---- Setting Dataframe ----')
    # create dataframe and save data to csv file
    posdataset = pl.DataFrame(aisdatalist)
    # dataset.to_csv(f'/Users/zultan/Downloads/pyais_position{filedate}.csv', index=False)
    staticdataset = pl.DataFrame(aisstaticlist)
    # staticdataset.to_csv(f'/Users/zultan/Downloads/pyais_static{filedate}.csv', index=False)

    # apply windows function
    # staticData = staticdataset.sort_values(by="ts").groupby("mmsi")
    # mmsi_asKeys = staticData.groups.keys()
    # mmsi_staticList = {}

    # for key in mmsi_asKeys:
    #     tmp_df = staticData.get_group(key).copy()
    #     tmp_df['next'] = tmp_df['ts'].shift(-1)
    #     tmp_df["next"] = tmp_df["next"].fillna(value=tmp_df["ts"])
    #     mmsi_staticList[str(key)] = tmp_df


    # clear up
    aisdatalist.clear()
    aisstaticlist.clear()

    # display quick summary
    print('---- Dataframe was built ----')
    print(posdataset.shape)
    print(posdataset.columns)
    print(staticdataset.shape)
    print(staticdataset.columns)    

    # re-index index colummn
    # dataset = dataset.drop(columns=['Unnamed: 0'], axis=1)
    # posdataset = posdataset.set_index(posdataset['ts'])
    # posdataset = posdataset.drop(columns=['ts'], axis=1)
    # staticdataset = staticdataset.set_index(staticdataset['ts'])
    # staticdataset = staticdataset.drop(columns=['ts'], axis=1)

    # Normalise data - merge position dataframe and static dataframe
    print('---- Normalising data ----')
    dataset = posdataset.join_asof(staticdataset, on="ts", by="mmsi")
    dataset.write_parquet(f'/Users/zultan/Downloads/pyais_polars{filedate}.parquet', use_pyarrow=True)
    print(dataset.shape)
    print(dataset.columns)

    # data analysis
    print('---- Analysing data in zone ----')

    # convert polars to pandas
    pddf = dataset.to_pandas()

    gdf = gpd.GeoDataFrame(pddf, geometry=gpd.points_from_xy(pddf.longitude, pddf.latitude), crs="EPSG:4326")
    geozone_df = get_geozone(1)

    vessel_in_zone = gpd.sjoin(gdf, geozone_df, how='inner', predicate='within')
    # output result
    df = pd.DataFrame(vessel_in_zone)
    print(df.shape)

    all_mmsi_withinzone = vessel_in_zone['mmsi'].unique()
    print(len(all_mmsi_withinzone))
    

    # calculate total processing time
    print('---- Complete ----')
    ellapseTime = time.time() - startTime
    print(ellapseTime)



if __name__ == '__main__':
    main()