from enum import Enum

class Sync_State(Enum):
    UTC_direct = 0
    UTC_indirect = 1
    Base_direct = 2
    Base_indirect = 3


class Slot_Timeout(Enum):
    This_was_the_last_transmission_in_this_slot = 0
    frames_are_left_until_slot_change = 1