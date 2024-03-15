from enum import Enum

class CommonIndicator(Enum):
        steady = 0
        decreasing = 1
        increasing = 2
        not_available = 3 

class CommonIndicator_yesno(Enum):
        no = 0
        yes = 1
        unknown = 2
        not_available = 3 