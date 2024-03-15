from enum import Enum


class EPFD(Enum):
    Undefined_default = 0
    GPS = 1
    GLONASS = 2
    Combined_GPS_and_GLONASS = 3
    Loran_C = 4
    Chayka = 5
    Integrated_navigation_system = 6
    Surveyed = 7
    Galileo = 8
    Reserved_1 = 9
    Reserved_2 = 10
    Reserved_3 = 11
    Reserved_4 = 12
    Reserved_5 = 13
    Reserved_6 = 14
    Internal_GNSS = 15