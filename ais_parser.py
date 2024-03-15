from ais_message_type import MessageType 
from ais_navigation_status import NavigationStatus 
from ais_shiptype import ShipType
from ais_epfd import EPFD
from ais_common_indicator import CommonIndicator, CommonIndicator_yesno
from ais_precipitation import Precipitation
from ais_beaufortscale import BeaufortScale
from ais_aid_type import Nav_Aid_Type
from ais_comm_state import Sync_State, Slot_Timeout


class Nmea():
    def binary_parser(binaryString, bitStart, bitLength, sign):
        binaryString = '0b' + binaryString[bitStart:bitStart+bitLength]
        #print(binaryString)
        return int(binaryString, 2) if sign == False else int(binaryString, 2) - (2**len(binaryString[2:]) * int(binaryString[0:3], 2)) 

    def six_bit_Character(binaryString, bitStart, bitLength, bitCharLength):
        binaryString = binaryString[bitStart:bitStart+bitLength]
        retString = ''

        while len(binaryString) >= bitCharLength:
            binaryCode = binaryString[0:bitCharLength]
            binaryString = binaryString[bitCharLength:]

            charCode = Nmea.binary_parser(binaryCode, 0, bitCharLength, False) 
            charCode = charCode+64 if charCode < 32 else charCode

            retString += chr(charCode)

        return retString.strip() if '@@@@@' not in retString else 'Not available'

    # Message Type : 1, 2, 3
    def ais_position_parser(binaryString):
        navStatus = Nmea.binary_parser(binaryString, 38, 4, False)  

        try:
            navStatusDesc = NavigationStatus(navStatus if navStatus <= 15 else 15).name.replace('_', ' ')
        except:
            print(f'Parse Content [Position]:: navStatusDesc: {navStatus}')

        rot = Nmea.binary_parser(binaryString, 42, 8, True)  
        rot = (rot/4.733)**2 if rot > 0 else -(rot/4.733)**2

        sog = Nmea.binary_parser(binaryString, 50, 10, False) 
        positionAccuracy = Nmea.binary_parser(binaryString, 60, 1, False)  

        syncState = Nmea.binary_parser(binaryString, 149, 2, False)
        slotTimeout = Nmea.binary_parser(binaryString, 151, 3, False)

        ais_position = {
            'navStatus': navStatus,
            'navStatusDesc': navStatusDesc,
            'rot': rot,
            'sog': sog/10.0,
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            'longitude': Nmea.binary_parser(binaryString, 61, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 89, 27, True) / 600000.0,
            'cog': Nmea.binary_parser(binaryString, 116, 12, False) / 10.0,
            'trueHeading': Nmea.binary_parser(binaryString, 128, 9, False), 
            'timeStamp': Nmea.binary_parser(binaryString, 137, 6, False), 
            'manoeuvre': Nmea.binary_parser(binaryString, 143, 2, False),
            'raimFlag': Nmea.binary_parser(binaryString, 148, 1, False),
            'radioStatus': Nmea.binary_parser(binaryString, 149, 19, False),
            'syncState': syncState,
            'syncStateDesc': Sync_State(syncState if syncState <= 3 else 0).name.replace('_', ' '),
            'slotTimeout': slotTimeout,
            'slotTimeoutDesc': str(slotTimeout) + " " + Slot_Timeout(slotTimeout if slotTimeout <= 1 else 1).name.replace("_", " "),
            'subMessage': Nmea.binary_parser(binaryString, 154, len(binaryString[154:]), False)
        }
        
        return ais_position    

    # Message Type : 4
    def ais_baseStation_parser(binaryString):

        epfd = Nmea.binary_parser(binaryString, 134, 4, False)
        accuracy = Nmea.binary_parser(binaryString, 78, 1, False)

        syncState = Nmea.binary_parser(binaryString, 149, 2, False)
        slotTimeout = Nmea.binary_parser(binaryString, 151, 3, False)

        ais_baseStation = {
            'utc_year': Nmea.binary_parser(binaryString, 38, 14, False),
            'utc_month': Nmea.binary_parser(binaryString, 52, 4, False),
            'utc_day': Nmea.binary_parser(binaryString, 56, 5, False),
            'utc_hour': Nmea.binary_parser(binaryString, 61, 5, False),
            'utc_minute': Nmea.binary_parser(binaryString, 66, 6, False),
            'utc_second': Nmea.binary_parser(binaryString, 72, 6, False),
            'fixQualityAccuracy': accuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if accuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            
            'longitude': Nmea.binary_parser(binaryString, 79, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 107, 27, True) / 600000.0,

            'epfd': epfd,
            'epfdDesc': EPFD(epfd if epfd <= 15 else 0).name.replace('_', ' '),

            'raimFlag': Nmea.binary_parser(binaryString, 148, 1, False),
            'radioStatus': Nmea.binary_parser(binaryString, 149, 19, False),

            'syncState': syncState,
            'syncStateDesc': Sync_State(syncState if syncState <= 3 else 0).name.replace('_', ' '),
            'slotTimeout': slotTimeout,
            'slotTimeoutDesc': str(slotTimeout) + " " + Slot_Timeout(slotTimeout if slotTimeout <= 1 else 1).name.replace("_", " "),
            'subMessage': Nmea.binary_parser(binaryString, 154, len(binaryString[154:]), False)            
        }

        return ais_baseStation

    # Message Type : 5
    def ais_static_parser(binaryString):
        shipType = Nmea.binary_parser(binaryString, 232, 8, False)

        ais_static = {
            'aisVersion': Nmea.binary_parser(binaryString, 38, 2, False),
            'imo': Nmea.binary_parser(binaryString, 40, 30, False),

            'callsign': Nmea.six_bit_Character(binaryString, 70, 42, 6),
            'shipName': Nmea.six_bit_Character(binaryString, 112, 120, 6),
            'shipType': shipType,
            'shipTypeDesc': ShipType(shipType if shipType<= 99 else 99).name.replace('_', " "),

            'to_bow': Nmea.binary_parser(binaryString, 240, 9, False),
            'to_stern': Nmea.binary_parser(binaryString, 249, 9, False),
            'to_port': Nmea.binary_parser(binaryString, 258, 6, False),
            'to_starboard': Nmea.binary_parser(binaryString, 264, 6, False),

            'epfd': Nmea.binary_parser(binaryString, 270, 4, False),
            'eta_month': Nmea.binary_parser(binaryString, 274, 4, False),
            'eta_day': Nmea.binary_parser(binaryString, 278, 5, False),
            'eta_hour': Nmea.binary_parser(binaryString, 283, 5, False),
            'eta_minute': Nmea.binary_parser(binaryString, 288, 6, False),
            'draught': Nmea.binary_parser(binaryString, 294, 8, False) / 10.0,

            'destination': Nmea.six_bit_Character(binaryString, 302, 120, 6),
            'dte': Nmea.binary_parser(binaryString, 422, 1, False),
        }

        return ais_static

    def ais_msg8_DAC1_FID31(binaryString):
        positionAccuracy = Nmea.binary_parser(binaryString, 105, 1, False)  
        airPressureTendency = Nmea.binary_parser(binaryString, 191, 2, False)
        waterLevelTrend = Nmea.binary_parser(binaryString, 213, 2, False)
        seaState = Nmea.binary_parser(binaryString, 322, 4, False)
        precipitation = Nmea.binary_parser(binaryString, 336, 3, False)
        ice = Nmea.binary_parser(binaryString, 348, 2, False)

        try:
            spare = Nmea.binary_parser(binaryString, 350, 10, False)
        except:
            print(f'Parse Content [ais_msg8_DAC1_FID31]:: binaryString length: {len(binaryString)}')


        ais_msg8_dac1_fid31 = {
            'longitude': Nmea.binary_parser(binaryString, 56, 25, True) / 60000.0,
            'latitude': Nmea.binary_parser(binaryString, 81, 24, True) / 60000.0,

            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            
            'utc_day': Nmea.binary_parser(binaryString, 106, 5, False),
            'utc_hour': Nmea.binary_parser(binaryString, 111, 5, False),
            'utc_minute': Nmea.binary_parser(binaryString, 116, 6, False),

            'avgWindSpeed': Nmea.binary_parser(binaryString, 122, 7, True),
            'windGust': Nmea.binary_parser(binaryString, 129, 7, True),
            'windDirection': Nmea.binary_parser(binaryString, 136, 9, False),
            'windGustDirection': Nmea.binary_parser(binaryString, 145, 9, False),
            'airTemperature': Nmea.binary_parser(binaryString, 154, 11, True),
            'relativeHumidity': Nmea.binary_parser(binaryString, 165, 7, False),
            'dewpoint': Nmea.binary_parser(binaryString, 172, 10, True),
            'airPressure': Nmea.binary_parser(binaryString, 182, 9, False),
            'airPressureTendency': airPressureTendency,
            'airPressureTendencyDesc': CommonIndicator(airPressureTendency if airPressureTendency <= 3 else 3).name.replace('_', ' '),
            'horizontalVisibility': Nmea.binary_parser(binaryString, 193, 8, False),
            'waterLevel': Nmea.binary_parser(binaryString, 201, 12, False),
            'waterLevelTrend': waterLevelTrend,
            'waterLevelTrendDesc': CommonIndicator(waterLevelTrend if waterLevelTrend <= 3 else 3).name.replace('_', ' '),
            'surfaceCurrentSpeed': Nmea.binary_parser(binaryString, 215, 8, False),
            'surfaceCurrentDirection': Nmea.binary_parser(binaryString, 223, 9, False),

            'surfaceCurrentSpeed_2': Nmea.binary_parser(binaryString, 232, 8, False),
            'surfaceCurrentDirection_2': Nmea.binary_parser(binaryString, 240, 9, False),
            'CurrentMeasureLevel_2': Nmea.binary_parser(binaryString, 249, 5, False),

            'surfaceCurrentSpeed_3': Nmea.binary_parser(binaryString, 254, 8, False),
            'surfaceCurrentDirection_3': Nmea.binary_parser(binaryString, 262, 9, False),
            'CurrentMeasureLevel_3': Nmea.binary_parser(binaryString, 271, 5, False),

            'significantWaveHeight': Nmea.binary_parser(binaryString, 276, 8, False),
            'wavePeriod': Nmea.binary_parser(binaryString, 284, 6, False),
            'waveDirection': Nmea.binary_parser(binaryString, 290, 9, False),

            'swellHeight': Nmea.binary_parser(binaryString, 299, 8, False),
            'swellPeriod': Nmea.binary_parser(binaryString, 307, 6, False),
            'swellDirection': Nmea.binary_parser(binaryString, 313, 9, False),

            'seaState': seaState,
            'seaStateDesc': BeaufortScale(seaState if seaState <= 15 else 13).name.replace('_', ' '),
            'waterTemperature': Nmea.binary_parser(binaryString, 326, 10, False),

            'precipitation': precipitation,
            'precipitationDesc': Precipitation(precipitation if precipitation <= 7 else 7).name.replace('_', ' '),

            'salinity': Nmea.binary_parser(binaryString, 339, 9, False),
            'ice': ice,
            'iceDesc': CommonIndicator_yesno(ice if ice <= 3 else 3).name.replace('_', ' ')
        }

        return ais_msg8_dac1_fid31

    # Water Level
    def ais_msg8_DAC200_FID24(binaryString):
        ais_msg8_dac200_fid24 = {
            'country': Nmea.six_bit_Character(binaryString, 56, 12, 6),
        }

        return ais_msg8_dac200_fid24


    def ais_msg6_DAC133_FID13(binaryString):
        ais_msg6_dac133_fid13 = {
            "empty": Nmea.binary_parser(binaryString, 88, 9, False) * 20,           # 0.02– 7.000M 20mm step * 20
            "full": Nmea.binary_parser(binaryString, 97, 9, False) * 20,            # 0.02– 7.000M 20mm step * 20
            "actual": Nmea.binary_parser(binaryString, 106, 9, False) * 20,         # 0.02– 7.000M 20mm step * 20
            "supply": Nmea.binary_parser(binaryString, 115, 1, False),              # 0: internal, 1: main
            "half": Nmea.binary_parser(binaryString, 116, 1, False),                # 0: less than half, 1: more than half
            "case_cover": Nmea.binary_parser(binaryString, 117, 1, False),          # 0: closed, 1: opened
            "battery": Nmea.binary_parser(binaryString, 118, 9, False) *0.05,       # *0.05
            "sonar": Nmea.binary_parser(binaryString, 127, 2, False) if len(binaryString) >= 129 else 0,       # 0: not installed, 1: installed OK, 2: installed Err
            "hoppers": Nmea.binary_parser(binaryString, 129, 4, False) if len(binaryString) >= 133 else 0,     # 0: sensors disabled
            "hopper1": Nmea.binary_parser(binaryString, 133, 1, False) if len(binaryString) >= 134 else 0,     # 0: open, 1: close
            "hopper2": Nmea.binary_parser(binaryString, 134, 1, False) if len(binaryString) >= 135 else 0,     # 0: open, 1: close
            "hopper3": Nmea.binary_parser(binaryString, 135, 1, False) if len(binaryString) >= 136 else 0,     # 0: open, 1: close
            "hopper4": Nmea.binary_parser(binaryString, 136, 1, False) if len(binaryString) >= 137 else 0,     # 0: open, 1: close
            "hopper5": Nmea.binary_parser(binaryString, 137, 1, False) if len(binaryString) >= 138 else 0,     # 0: open, 1: close
            "hopper6": Nmea.binary_parser(binaryString, 138, 1, False) if len(binaryString) >= 139 else 0,     # 0: open, 1: close
            "hopper7": Nmea.binary_parser(binaryString, 139, 1, False) if len(binaryString) >= 140 else 0,     # 0: open, 1: close
            "hopper8": Nmea.binary_parser(binaryString, 140, 1, False) if len(binaryString) >= 141 else 0,     # 0: open, 1: close
        }

        return ais_msg6_dac133_fid13


    def ais_msg6_lighthouseMalaysia_parser(binaryString):
        ais_msg6_lighthouseMalaysia = {
            "volt_int": Nmea.binary_parser(binaryString, 88, 10, False) *0.05,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex1": Nmea.binary_parser(binaryString, 98, 10, False) *0.05,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex2": Nmea.binary_parser(binaryString, 108, 10, False) *0.05,          # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit  
            "racon": Nmea.binary_parser(binaryString, 118, 2, False) if len(binaryString) >= 120 else 0,           # 0: not installed, 1: not monitor, 2: operating, 3: error
            "light": Nmea.binary_parser(binaryString, 120, 2, False) if len(binaryString) >= 122 else 0,           # 0: not installed/monitor, 1: light on, 2: light off, 3: error
            "health": Nmea.binary_parser(binaryString, 122, 1, False) if len(binaryString) >= 123 else 0,          # 0: good health, 1: alarm 
            "beat": Nmea.binary_parser(binaryString, 123, 1, False) if len(binaryString) >= 124 else 0,                     # 0: tick, 1: tock
            "lantern_batt": Nmea.binary_parser(binaryString, 124, 2, False) if len(binaryString) >= 126 else 0,             # 0: unknown, 1: bad, 2: low, 3: good
            "lantern": Nmea.binary_parser(binaryString, 126, 2, False) if len(binaryString) >= 128 else 0,                  # 0: no light, 1: primary, 2: secondary, 3: emergency
            "ambient": Nmea.binary_parser(binaryString, 128, 2, False) if len(binaryString) >= 130 else 0,                  # 0: not LDR, 1: dark, 2: dim, 3: bright
            "hatch_door": Nmea.binary_parser(binaryString, 130, 1, False) if len(binaryString) >= 131 else 0,                     # 0: close, 1: open
            "off_pos": Nmea.binary_parser(binaryString, 131, 1, False) if len(binaryString) >= 132 else 0,                  # 0: on position, 1: off position
        }

        return ais_msg6_lighthouseMalaysia

    def ais_msg6_lighthouseMalaysia2_parser(binaryString):
        ais_msg6_lighthouseMalaysia2 = {
            "volt_int": Nmea.binary_parser(binaryString, 88, 9, False) *0.1,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex1": Nmea.binary_parser(binaryString, 97, 9, False) *0.1,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex2": Nmea.binary_parser(binaryString, 106, 9, False) *0.1,          # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "off_pos": Nmea.binary_parser(binaryString, 115, 1, False) if len(binaryString) >= 116 else 0,         # 0: on position, 1: off position
            "ambient": Nmea.binary_parser(binaryString, 116, 2, False) if len(binaryString) >= 118 else 0,         # 0: not LDR, 1: dark, 2: dim, 3: bright
            "racon": Nmea.binary_parser(binaryString, 118, 2, False) if len(binaryString) >= 120 else 0,           # 0: not installed, 1: not monitor, 2: operating, 3: error
            "light": Nmea.binary_parser(binaryString, 120, 2, False) if len(binaryString) >= 122 else 0,           # 0: not installed/monitor, 1: light on, 2: light off, 3: error
            "health": Nmea.binary_parser(binaryString, 122, 1, False) if len(binaryString) >= 123 else 0,          # 0: good health, 1: alarm 

            "beat": Nmea.binary_parser(binaryString, 123, 1, False) if len(binaryString) >= 124 else 0,                     # 0: tick, 1: tock
            "main_lantern_cond": Nmea.binary_parser(binaryString, 124, 1, False) if len(binaryString) >= 125 else 0,        # 0: normal, 1: fail
            "main_lantern_stat": Nmea.binary_parser(binaryString, 125, 1, False) if len(binaryString) >= 126 else 0,        # 0: off, 1: on
            "stdby_lantern_cond": Nmea.binary_parser(binaryString, 126, 1, False) if len(binaryString) >= 127 else 0,       # 0: normal, 1: fail
            "stdby_lantern_stat": Nmea.binary_parser(binaryString, 127, 1, False) if len(binaryString) >= 128 else 0,       # 0: off, 1: on
            "emerg_lantern_cond": Nmea.binary_parser(binaryString, 128, 1, False) if len(binaryString) >= 129 else 0,       # 0: normal, 1: fail 
            "emerg_lantern_stat": Nmea.binary_parser(binaryString, 129, 1, False) if len(binaryString) >= 130 else 0,       # 0: off, 1: on
            "opticA_drive_stat": Nmea.binary_parser(binaryString, 130, 1, False) if len(binaryString) >= 131 else 0,        # 0: off, 1: on
            "opticA_drive_cond": Nmea.binary_parser(binaryString, 131, 1, False) if len(binaryString) >= 132 else 0,        # 0: normal, 1: fail
            "opticB_drive_stat": Nmea.binary_parser(binaryString, 132, 1, False) if len(binaryString) >= 133 else 0,        # 0: off, 1: on                        
            "opticB_drive_cond": Nmea.binary_parser(binaryString, 133, 1, False) if len(binaryString) >= 134 else 0,        # 0: normal, 1: fail
            "hatch_door": Nmea.binary_parser(binaryString, 134, 1, False) if len(binaryString) >= 135 else 0,               # 0: close, 1: open
            "main_power": Nmea.binary_parser(binaryString, 135, 1, False) if len(binaryString) >= 136 else 0,               # 0: off, 1: on
            "bms_cond": Nmea.binary_parser(binaryString, 136, 1, False) if len(binaryString) >= 137 else 0,                 # 0: normal, 1: fail 
        }

        return ais_msg6_lighthouseMalaysia2        

    def ais_msg6_lighthouseMalaysia4_parser(binaryString):
        ais_msg6_lighthouseMalaysia4 = {
            "volt_int": Nmea.binary_parser(binaryString, 88, 9, False) *0.05,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex1": Nmea.binary_parser(binaryString, 97, 9, False) *0.05,           # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "volt_ex2": Nmea.binary_parser(binaryString, 106, 9, False) *0.2,          # 0.1– 51.1V 0.1V step Supply voltage to AIS ATON Unit
            "off_pos": Nmea.binary_parser(binaryString, 115, 1, False) if len(binaryString) >= 116 else 0,         # 0: on position, 1: off position
            "ambient": Nmea.binary_parser(binaryString, 116, 2, False) if len(binaryString) >= 118 else 0,         # 0: not LDR, 1: dark, 2: dim, 3: bright
            "racon": Nmea.binary_parser(binaryString, 118, 2, False) if len(binaryString) >= 120 else 0,           # 0: not installed, 1: not monitor, 2: operating, 3: error
            "light": Nmea.binary_parser(binaryString, 120, 2, False) if len(binaryString) >= 122 else 0,           # 0: not installed/monitor, 1: light on, 2: light off, 3: error
            "health": Nmea.binary_parser(binaryString, 122, 1, False) if len(binaryString) >= 123 else 0,          # 0: good health, 1: alarm 

            "beat": Nmea.binary_parser(binaryString, 123, 1, False) if len(binaryString) >= 124 else 0,                     # 0: tick, 1: tock
            "alarm_active": Nmea.binary_parser(binaryString, 124, 1, False) if len(binaryString) >= 125 else 0,             # 0: no, 1: yes
            "buoy_led_power": Nmea.binary_parser(binaryString, 125, 1, False) if len(binaryString) >= 126 else 0,           # 0: no, 1: yes
            "buoy_low_vin": Nmea.binary_parser(binaryString, 126, 1, False) if len(binaryString) >= 127 else 0,             # 0: no, 1: yes
            "buoy_photocell": Nmea.binary_parser(binaryString, 127, 1, False) if len(binaryString) >= 128 else 0,           # 0: no, 1: yes
            "buoy_temp": Nmea.binary_parser(binaryString, 128, 1, False) if len(binaryString) >= 129 else 0,                # 0: no, 1: yes
            "buoy_force_off": Nmea.binary_parser(binaryString, 129, 1, False) if len(binaryString) >= 130 else 0,           # 0: no, 1: yes
            "buoy_islight": Nmea.binary_parser(binaryString, 130, 1, False) if len(binaryString) >= 131 else 0,             # 0: no, 1: yes
            "buoy_errled_short": Nmea.binary_parser(binaryString, 131, 1, False) if len(binaryString) >= 132 else 0,        # 0: no, 1: yes
            "buoy_errled_open": Nmea.binary_parser(binaryString, 132, 1, False) if len(binaryString) >= 133 else 0,         # 0: no, 1: yes                       
            "buoy_errled_voltlow": Nmea.binary_parser(binaryString, 133, 1, False) if len(binaryString) >= 134 else 0,      # 0: no, 1: yes
            "buoy_errled_vinlow": Nmea.binary_parser(binaryString, 134, 1, False) if len(binaryString) >= 135 else 0,       # 0: no, 1: yes
            "buoy_errled_power": Nmea.binary_parser(binaryString, 135, 1, False) if len(binaryString) >= 136 else 0,        # 0: no, 1: yes
            "buoy_adjmaxpower": Nmea.binary_parser(binaryString, 136, 1, False) if len(binaryString) >= 137 else 0,         # 0: no, 1: yes 
            "buoy_sensor_interrupt": Nmea.binary_parser(binaryString, 137, 1, False) if len(binaryString) >= 138 else 0,    # 0: no, 1: yes
            "buoy_solarcharging": Nmea.binary_parser(binaryString, 138, 1, False) if len(binaryString) >= 139 else 0,       # 0: no, 1: yes
        }

        return ais_msg6_lighthouseMalaysia4   


    def ais_msg6_lighthouse_parser(binaryString):
        ais_msg6_lighthouse = {
            "volt_int": Nmea.binary_parser(binaryString, 88, 10, False) *0.05,           # 00.05-36V, 0.05V step Supply voltage to AIS Unit 0 = Not Used
            "volt_ex1": Nmea.binary_parser(binaryString, 98, 10, False) *0.05,           # 0.05-36V, 0.05V step Supply voltage to AIS Unit 0 = Not Used
            "volt_ex2": Nmea.binary_parser(binaryString, 108, 10, False) *0.05,          # 0.05-36V, 0.05V step Supply voltage to AIS Unit 0 = Not Used
            "racon": Nmea.binary_parser(binaryString, 118, 2, False),                   # 0: not installed, 1: not monitor, 2: operating, 3: error
            "light": Nmea.binary_parser(binaryString, 120, 2, False),                   # 0: not installed/monitor, 1: light on, 2: light off, 3: error
            "health": Nmea.binary_parser(binaryString, 122, 1, False),                  # 0: good health, 1: alarm
            
            "stat_ext7": Nmea.binary_parser(binaryString, 123, 1, False) if len(binaryString) >= 124 else 0,     # 0: off, 1: on
            "stat_ext6": Nmea.binary_parser(binaryString, 124, 1, False) if len(binaryString) >= 125 else 0,     # 0: off, 1: on
            "stat_ext5": Nmea.binary_parser(binaryString, 125, 1, False) if len(binaryString) >= 126 else 0,     # 0: off, 1: on
            "stat_ext4": Nmea.binary_parser(binaryString, 126, 1, False) if len(binaryString) >= 127 else 0,     # 0: off, 1: on
            "stat_ext3": Nmea.binary_parser(binaryString, 127, 1, False) if len(binaryString) >= 128 else 0,     # 0: off, 1: on
            "stat_ext2": Nmea.binary_parser(binaryString, 128, 1, False) if len(binaryString) >= 129 else 0,     # 0: off, 1: on
            "stat_ext1": Nmea.binary_parser(binaryString, 129, 1, False) if len(binaryString) >= 120 else 0,     # 0: off, 1: on
            "stat_ext0": Nmea.binary_parser(binaryString, 130, 1, False) if len(binaryString) >= 131 else 0,     # 0: off, 1: on
            "off_pos": Nmea.binary_parser(binaryString, 131, 1, False) if len(binaryString) >= 132 else 0,       # 0: on position, 1: off position
        }

        return ais_msg6_lighthouse


    def ais_msg6_ais_zenilite_parser(binaryString):
        ais_msg6_zenilite = {
            "app_id": Nmea.binary_parser(binaryString, 88, 16, False),
            "voltage": Nmea.binary_parser(binaryString, 104, 12, False),
            "current": Nmea.binary_parser(binaryString, 116, 10, False),
            "supply_type": Nmea.binary_parser(binaryString, 126, 1, False) if len(binaryString) >= 127 else 0,     # 0: ac, 1: dc
            "light": Nmea.binary_parser(binaryString, 127, 1, False) if len(binaryString) >= 128 else 0,           # 0: off, 1: on
            "batt_stat": Nmea.binary_parser(binaryString, 128, 1, False) if len(binaryString) >= 129 else 0,       # 0: good, 1: low
            "off_pos": Nmea.binary_parser(binaryString, 129, 1, False) if len(binaryString) >= 130 else 0,         # 0: on position, 1: off position
        }

        return ais_msg6_zenilite


    # Message Type : 6
    def ais_aton_parser(binaryString):
        seqno = Nmea.binary_parser(binaryString, 38, 2, False)
        dest_mmsi = Nmea.binary_parser(binaryString, 40, 30, False)
        retransmit = Nmea.binary_parser(binaryString, 70, 1, False)

        # Designated Area Code
        dac = Nmea.binary_parser(binaryString, 72, 10, False)
        # Functional Id
        fid = Nmea.binary_parser(binaryString, 82, 6, False)

        ais_binBroadcast = {
            'seqno': seqno,
            'dest_mmsi': dest_mmsi,
            'retransmit': retransmit,            
            'dac': dac,
            'fid': fid,
        }


        # zenilite
        # if dac == 10 and fid == 6:
        #     ais_zenilite = Nmea.ais_msg6_ais_zenilite_parser(binaryString)
        #     ais_binBroadcast.update(ais_zenilite)

        # DDMS
        if dac == 133 and fid == 13:
            ais_msg6_dac133_fid13 = Nmea.ais_msg6_DAC133_FID13(binaryString)
            ais_binBroadcast.update(ais_msg6_dac133_fid13)

        # AtoN monitoring data (General Lighthouse)
        if (dac == 250 and fid == 10):
            ais_msg6_lighthouse = Nmea.ais_msg6_lighthouse_parser(binaryString)
            ais_binBroadcast.update(ais_msg6_lighthouse)       

        # AtoN monitoring data (Light Beacon Application)
        if (dac == 533 and fid == 1) or (dac == 235 and fid == 10) :
            ais_msg6_lighthouseMalaysia = Nmea.ais_msg6_lighthouseMalaysia_parser(binaryString)
            ais_binBroadcast.update(ais_msg6_lighthouseMalaysia)

        # AtoN monitoring data (Renewable Energy Power Source)
        if (dac == 533 and fid == 2):
            ais_msg6_lighthouseMalaysia2 = Nmea.ais_msg6_lighthouseMalaysia2_parser(binaryString)
            ais_binBroadcast.update(ais_msg6_lighthouseMalaysia2)

        # AtoN monitoring data (Buoy or Small Light Beacon)
        if (dac == 533 and fid == 4):
            ais_msg6_lighthouseMalaysia4 = Nmea.ais_msg6_lighthouseMalaysia4_parser(binaryString)
            ais_binBroadcast.update(ais_msg6_lighthouseMalaysia4)


        return ais_binBroadcast



    # Message Type : 8
    def ais_binaryBroadcast_parser(binaryString):
        # Designated Area Code
        dac = Nmea.binary_parser(binaryString, 40, 10, False)
        # Functional Id
        fid = Nmea.binary_parser(binaryString, 50, 6, False)

        ais_binBroadcast = {
            'dac': dac,
            'fid': fid
        }

        if dac == 1 and fid == 29:
            ais_binBroadcast.update({'linkId': Nmea.binary_parser(binaryString, 56, 10, False)})
            ais_binBroadcast.update({'text': Nmea.six_bit_Character(binaryString, 66, len(binaryString[66:]), 6)})
        elif dac == 1 and fid == 31:
            ais_msg8_dac1_fid31 = Nmea.ais_msg8_DAC1_FID31(binaryString)
            ais_binBroadcast.update(ais_msg8_dac1_fid31)
        # elif dac == 200 and fid == 24:
        #     ais_msg8_dac200_fid24 = Nmea.ais_msg8_DAC200_FID24(binaryString)
        #     ais_binBroadcast.update(ais_msg8_dac200_fid24)

        return ais_binBroadcast


    # Message Type 9: Standard SAR Aircraft Position Report
    def ais_aircraftPosition_parser(binaryString):
        positionAccuracy = Nmea.binary_parser(binaryString, 60, 1, False)  

        syncState = Nmea.binary_parser(binaryString, 148, 2, False)
        slotTimeout = Nmea.binary_parser(binaryString, 150, 3, False)

        ais_aircraftPosition = {
            'altitude': Nmea.binary_parser(binaryString, 38, 12, False),
            'sog': Nmea.binary_parser(binaryString, 50, 10, False) / 10.0,
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            'longitude': Nmea.binary_parser(binaryString, 61, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 89, 27, True) / 600000.0,      
            'cog': Nmea.binary_parser(binaryString, 116, 12, False) / 10.0,     
            'timeStamp': Nmea.binary_parser(binaryString, 128, 6, False), 
            'regional': Nmea.binary_parser(binaryString, 134, 8, False), 
            'dte': Nmea.binary_parser(binaryString, 142, 1, False),
            'assigned': Nmea.binary_parser(binaryString, 146, 1, False),
            'raimFlag': Nmea.binary_parser(binaryString, 147, 1, False),
            'radioStatus': Nmea.binary_parser(binaryString, 148, 20, False),
            'syncState': syncState,
            'syncStateDesc': Sync_State(syncState if syncState <= 3 else 0).name.replace('_', ' '),
            'slotTimeout': slotTimeout,
            'slotTimeoutDesc': str(slotTimeout) + " " + Slot_Timeout(slotTimeout if slotTimeout <= 1 else 1).name.replace("_", " "),
            'subMessage': Nmea.binary_parser(binaryString, 153, len(binaryString[153:]), False)
        }

        return ais_aircraftPosition

    # Message Type 12: Addressed Safety-Related Message
    def ais_addressSafety_parser(binaryString):
        ais_addressSafety = {
            'seqNo': Nmea.binary_parser(binaryString, 38, 2, False),
            'dest_mmsi': Nmea.binary_parser(binaryString, 40, 30, False),
            'retransmit': Nmea.binary_parser(binaryString, 70, 1, False),
            'text': Nmea.six_bit_Character(binaryString, 72, len(binaryString[72:]), 6),
        }

        return ais_addressSafety


    # Message Type 14: Safety-Related Broadcast Message
    def ais_SafetyBroadcast_parser(binaryString):
        ais_SafetyBroadcast = {
            'text': Nmea.six_bit_Character(binaryString, 40, len(binaryString[40:]), 6),
        }

        return ais_SafetyBroadcast


    # Message Type 15: Interrogation
    def ais_interrogation_parser(binaryString):
        ais_interrogation = {
            'mmsi1': Nmea.binary_parser(binaryString, 40, 30, False),
            'type1_1': Nmea.binary_parser(binaryString, 70, 6, False),
            'offset1_1': Nmea.binary_parser(binaryString, 76, 12, False),

            'type1_2': Nmea.binary_parser(binaryString, 90, 6, False),
            'offset1_2': Nmea.binary_parser(binaryString, 96, 12, False),   
            
            'mmsi2': Nmea.binary_parser(binaryString, 110, 30, False),
            'type2_1': Nmea.binary_parser(binaryString, 140, 6, False),
            'offset2_1': Nmea.binary_parser(binaryString, 146, 12, False),                 
        }

        return ais_interrogation

    # Message Type 16: Assignment Mode Command
    def ais_AssignmentMode_parser(binaryString):
        ais_AssignmentMode = {
            'mmsi1': Nmea.binary_parser(binaryString, 40, 30, False),
            'offset1': Nmea.binary_parser(binaryString, 70, 12, False),
            'increment1': Nmea.binary_parser(binaryString, 82, 10, False),  

            'mmsi2': Nmea.binary_parser(binaryString, 92, 30, False),
            'offset2': Nmea.binary_parser(binaryString, 122, 12, False),
            'increment2': Nmea.binary_parser(binaryString, 134, 10, False),                    
        }

        return 
        

    # Message Type 17: DGNSS Broadcast Binary Message
    def ais_DGNSS_parser(binaryString):
        data = Nmea.six_bit_Character(binaryString, 80, len(binaryString[80:]), 6)

        ais_DGNSS = {
            'longitude': Nmea.binary_parser(binaryString, 40, 18, True),
            'latitude': Nmea.binary_parser(binaryString, 58, 17, True),  
            'data': data                  
        }

        return ais_DGNSS
        

    # Message Type 18: Standard Class B CS Position Report
    def ais_classB_position_parser(binaryString):
        positionAccuracy = Nmea.binary_parser(binaryString, 56, 1, False) 

        ais_classB_position = {
            'reserved': Nmea.binary_parser(binaryString, 38, 8, False),
            'sog': Nmea.binary_parser(binaryString, 46, 10, False) / 10.0,  
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",

            'longitude': Nmea.binary_parser(binaryString, 57, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 85, 27, True) / 600000.0, 
            'cog': Nmea.binary_parser(binaryString, 112, 12, False) / 10.0,     
            'trueHeading': Nmea.binary_parser(binaryString, 124, 9, False), 
            'timeStamp': Nmea.binary_parser(binaryString, 133, 6, False),   
            'regional': Nmea.binary_parser(binaryString, 139, 2, False), 

            'cs': Nmea.binary_parser(binaryString, 141, 1, False),
            'display': Nmea.binary_parser(binaryString, 142, 1, False),   
            'dsc': Nmea.binary_parser(binaryString, 143, 1, False),   
            'band': Nmea.binary_parser(binaryString, 144, 1, False),   
            'msg22': Nmea.binary_parser(binaryString, 145, 1, False),   
            'assigned': Nmea.binary_parser(binaryString, 146, 1, False),   
            'raimFlag': Nmea.binary_parser(binaryString, 147, 1, False),
            'radioStatus': Nmea.binary_parser(binaryString, 148, 20, False)
        }

        return ais_classB_position


    # Message Type 19: Extended Class B CS Position Report
    def ais_classB_positionX_parser(binaryString):
        positionAccuracy = Nmea.binary_parser(binaryString, 56, 1, False) 
        shipType = Nmea.binary_parser(binaryString, 263, 8, False)
        epfd = Nmea.binary_parser(binaryString, 301, 4, False)

        ais_classB_positionX = {
            'reserved': Nmea.binary_parser(binaryString, 38, 8, False),
            'sog': Nmea.binary_parser(binaryString, 46, 10, False) / 10.0,  
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",

            'longitude': Nmea.binary_parser(binaryString, 57, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 85, 27, True) / 600000.0, 
            'cog': Nmea.binary_parser(binaryString, 112, 12, False) / 10.0,     
            'trueHeading': Nmea.binary_parser(binaryString, 124, 9, False), 
            'timeStamp': Nmea.binary_parser(binaryString, 133, 6, False),   
            'regional': Nmea.binary_parser(binaryString, 139, 4, False), 

            'shipName': Nmea.six_bit_Character(binaryString, 143, 120, 6),
            'shipType': shipType,
            'shipTypeDesc': ShipType(shipType if shipType<= 99 else 99).name.replace('_', " "),
            'to_bow': Nmea.binary_parser(binaryString, 271, 9, False),
            'to_stern': Nmea.binary_parser(binaryString, 280, 9, False),
            'to_port': Nmea.binary_parser(binaryString, 289, 6, False),
            'to_starboard': Nmea.binary_parser(binaryString, 295, 6, False),

            'epfd': epfd,
            'epfdDesc': EPFD(epfd if epfd <= 15 else 0).name.replace('_', ' '),

            'raimFlag': Nmea.binary_parser(binaryString, 305, 1, False),
            'dte': Nmea.binary_parser(binaryString, 306, 1, False),   
            'assigned': Nmea.binary_parser(binaryString, 307, 1, False)
        }

        return ais_classB_positionX

    # Message Type 21: Aid-to-Navigation Report 
    def ais_aid_navigation_parser(binaryString):
        aidType = Nmea.binary_parser(binaryString, 38, 5, False)
        positionAccuracy = Nmea.binary_parser(binaryString, 163, 1, False) 
        epfd = Nmea.binary_parser(binaryString, 249, 4, False)

        ais_aid_nav = {
            'aidType': aidType,
            'aidTypeDesc': Nav_Aid_Type(aidType if aidType <= 31 else 0).name.replace('_', ' '),
            'aidName': Nmea.six_bit_Character(binaryString, 43, 120, 6),
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            
            'longitude': Nmea.binary_parser(binaryString, 164, 28, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 192, 27, True) / 600000.0, 

            'to_bow': Nmea.binary_parser(binaryString, 219, 9, False),
            'to_stern': Nmea.binary_parser(binaryString, 228, 9, False),
            'to_port': Nmea.binary_parser(binaryString, 237, 6, False),
            'to_starboard': Nmea.binary_parser(binaryString, 243, 6, False),

            'epfd': epfd,
            'epfdDesc': EPFD(epfd if epfd <= 15 else 0).name.replace('_', ' '),
            'utc_second': Nmea.binary_parser(binaryString, 253, 6, False),
            'off_position': Nmea.binary_parser(binaryString, 259, 1, False),
            'regional': Nmea.binary_parser(binaryString, 260, 8, False),
            'raimFlag': Nmea.binary_parser(binaryString, 268, 1, False),
            'virtualAid': Nmea.binary_parser(binaryString, 269, 1, False),
            'assigned': Nmea.binary_parser(binaryString, 270, 1, False)
        }

        if len(binaryString[271:]) > 272 + 20:
            ext_name = Nmea.six_bit_Character(binaryString, 272, len(binaryString[272:]), 6),
            ais_aid_nav.update({'ext_name': ext_name})

        return ais_aid_nav


    # Message Type 24: Static Data Report
    def ais_static_report_parser(binaryString):
        shipType = Nmea.binary_parser(binaryString, 40, 8, False)
        
        if len(binaryString) != 168 :           # 162
            ais_static_report = {
                'portNo': Nmea.binary_parser(binaryString, 38, 2, False),
                'shipName': Nmea.six_bit_Character(binaryString, 40, 120, 6)
            }
        else:                                   # 168
            mmsi = Nmea.binary_parser(binaryString, 8, 30, False) 

            ais_static_report = {
                'partNo': Nmea.binary_parser(binaryString, 38, 2, False),
                'shipType': shipType,
                'shipTypeDesc': ShipType(shipType if shipType<= 99 else 99).name.replace('_', " "),
                'vendor': Nmea.six_bit_Character(binaryString, 48, 18, 6),
                'model': Nmea.binary_parser(binaryString, 66, 4, False),
                'serial': Nmea.binary_parser(binaryString, 70, 20, False),
                'callsign': Nmea.six_bit_Character(binaryString, 90, 42, 6),
            }

            if mmsi == 0 :
                ais_static_report.update({'motherShip_mmsi': Nmea.binary_parser(binaryString, 132, 30, False)})
            else:
                ais_static_info = {
                    'to_bow': Nmea.binary_parser(binaryString, 132, 9, False),
                    'to_stern': Nmea.binary_parser(binaryString, 141, 9, False),
                    'to_port': Nmea.binary_parser(binaryString, 150, 6, False),
                    'to_starboard': Nmea.binary_parser(binaryString, 156, 6, False)
                }

                ais_static_report.update(ais_static_info)


        return ais_static_report


    # Message Type 27: Long Range AIS Broadcast message
    def ais_long_range_broadcast_parser(binaryString):
        positionAccuracy = Nmea.binary_parser(binaryString, 38, 1, False) 
        navStatus = Nmea.binary_parser(binaryString, 40, 4, False) 

        ais_long_range_broadcast = {
            'positionAccuracy': positionAccuracy,
            'positionAccuracyDesc': "an unaugmented GNSS fix with accuracy > 10m" if positionAccuracy == 0 else "a DGPS-quality fix with an accuracy of < 10ms",
            'raimFlag': Nmea.binary_parser(binaryString, 39, 1, False),
            'navStatus': navStatus,
            'navStatusDesc': NavigationStatus(navStatus if navStatus <= 15 else 15).name.replace('_', ' '),
            'longitude': Nmea.binary_parser(binaryString, 44, 18, True) / 600000.0,
            'latitude': Nmea.binary_parser(binaryString, 62, 17, True) / 600000.0,  
            'sog': Nmea.binary_parser(binaryString, 79, 6, False) / 10.0,
            'cog': Nmea.binary_parser(binaryString, 85, 9, False) / 10.0,
            'gnss': Nmea.binary_parser(binaryString, 94, 1, False)         
        }

        return ais_long_range_broadcast