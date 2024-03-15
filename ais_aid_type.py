from enum import Enum

class Nav_Aid_Type(Enum):
    Default_or_Type_of_Aid_to_Navigation_not_specified = 0
    Reference_point = 1
    RACON_radar_transponder_marking_a_navigation_hazard = 2
    Fixed_structure_off_shore = 3
    Reserved_for_future_use = 4
    Light_without_sectors = 5
    Light_with_sectors = 6
    Leading_Light_Front = 7
    Leading_Light_Rear = 8
    Beacon_Cardinal_N = 9
    Beacon_Cardinal_E = 10
    Beacon_Cardinal_S = 11
    Beacon_Cardinal_W = 12
    Beacon_Port_hand = 13
    Beacon_Starboard_hand = 14
    Beacon_Preferred_Channel_port_hand = 15
    Beacon_Preferred_Channel_starboard_hand = 16
    Beacon_Isolated_danger = 17
    Beacon_Safe_water = 18
    Beacon_Special_mark = 19
    Cardinal_Mark_N = 20
    Cardinal_Mark_E = 21
    Cardinal_Mark_S = 22
    Cardinal_Mark_W = 23
    Port_hand_Mark = 24
    Starboard_hand_Mark = 25
    Preferred_Channel_Port_hand = 26
    Preferred_Channel_Starboard_hand = 27
    Isolated_danger = 28
    Safe_Water = 29
    Special_Mark = 30
    Light_Vessel_LANBY_Rigs = 31
