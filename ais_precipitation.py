from enum import Enum

class Precipitation(Enum):
    reserved = 0
    rain = 1
    thunderstorm = 2
    freezing_rain =3
    mixed_ice = 4
    snow = 5
    reserve = 6
    not_available_or_default = 7