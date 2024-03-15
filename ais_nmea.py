import math
import json
from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_parser import *


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



# !ABVDM,1,1,7,A,15R9eN001n7DHvT13w0TBSM>00Rm,0*54
# !ABVDM,1,1,2,B,37likkpOh27M3ud0Veic9as801i@,0*12
# !ABVDM,1,1,9,A,4055DwivO63307<PsL2H<G700D10,0*62
# !ABVDM,2,1,9,A,58I3mA82@Es3UKOOGB0l4E9<f1L4hhU>22222217H147I?610K54480CPj3l,0*19
# !ABVDM,2,2,9,A,PAiH8888880,2*16
# !AIVDM,1,1,,A,805GdVh0GjuoMp2?>h0AP@=UdB06EuOwgrBGwnSwe7wvlOwwsAwwnSGmwvh0,0*67
# !ABVDM,1,1,7,A,90007thcP07@nkR1jMJ@0>h20@S3,0*43
# !ABVDM,1,1,8,A,ENm>OAt:0W5:W3h9PTVPh1:Wdh@4=lSP1<j4000003v010,4*5F
# !ABVDM,1,1,1,A,C7tc>Hh05Ao0L20LREAmCwv0P2=1aiQW0=1111111110?1D5310P,0*40
# !ABVDM,1,1,7,B,B8HsF90009nTJO0;Pb803wjTkP06,0*67
# !AIVDM,1,1,,A,H7tCijTt00`0000qP=8EPm1pA668,0*49
# !ABVDM,1,1,6,A,H8HtV6QTF0th@D0000000000000,2*6E

# !ABVDM,1,1,3,A,6>m><PH000clQDB?AA`<P@0P,0*1A
# !ABVDM,1,1,0,B,6>m>=9D000clQD528=00W<0,2*74
# !ABVDM,1,1,1,A,6qt6UI8000cl8DnfUbgN;`H,2*79

# !ABVDM,1,1,4,A,6>m>=>l000clQD52Tc00W40,2*66
# !ABVDM,1,1,4,A,6Nm><1l000clQD4wl300CF0,2*52
# !ABVDM,1,1,2,A,6>m>=e@000cl>da2@500000,2*2D

ais_array = []
ais_array.append('!ABVDM,1,1,3,A,6>m><PH000clQDB?AA`<P@0P,0*1A')
# ais_array.append('!ABVDM,2,2,9,A,PAiH8888880,2*16')

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
                exit()
        

        # validate checksum
        if not check_ais_checksum(ais_msg=ais_msg):
            print('[ERROR ::] Invalid AIS. Checksum error.')
            exit()

        # validate previous ais package
        if prev_package :
            p_ais = prev_package.split(',')
            p_total_package = int(p_ais[1])
            p_package_no = int(p_ais[2])
            p_package_id = int(p_ais[3]) if p_ais[3] else 0

            if total_package != p_total_package or p_package_no != package_no-1 or p_package_id != package_id:
                print('[ERROR ::] Invalid AIS. Package not in sequence.')
                exit()                 

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
    print(package_data)
    value_list = [value for value in package_data.values()]
    print(value_list)
    key_list = [value for value in package_data.keys()]
    print(key_list)

else:
    print('[ERROR ::] No package found.')    




