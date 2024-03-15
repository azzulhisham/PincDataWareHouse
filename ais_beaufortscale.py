from enum import Enum

class BeaufortScale(Enum):
    Flat = 0
    Ripples_without_crests = 1
    Small_wavelets = 2
    Large_wavelets = 3
    Small_waves = 4
    Moderate_longer_waves = 5
    Large_waves_with_foam_crests_and_some_spray = 6
    Sea_heaps_up_and_foam_begins_to_streak = 7
    Moderately_high_waves_with_breaking_crests_forming_spindrift = 8
    High_waves_with_dense_foam = 9
    Very_high_waves = 10
    Exceptionally_high_wavesm = 11
    Huge_waves = 12
    Not_available = 13
    spare_1 = 14
    spare_2 = 15