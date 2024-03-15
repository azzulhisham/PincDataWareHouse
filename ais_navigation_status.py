from enum import Enum


class NavigationStatus(Enum):
        Under_way_using_engine = 0
        At_anchor = 1
        Not_under_command = 2
        Restricted_manoeuverability = 3
        Constrained_by_her_draught = 4
        Moored = 5
        Aground = 6
        Engaged_in_Fishing = 7
        Under_way_sailing = 8
        Reserved_for_future_amendment_of_Navigational_Status_for_HSC = 9
        Reserved_for_future_amendment_of_Navigational_Status_for_WIG = 10
        Power_driven_vessel_towing_astern_on_regional_use = 11
        Power_driven_vessel_pushing_ahead_or_towing_alongside_regional_use = 12
        Reserved_for_future_use = 13
        AIS_SART_is_active = 14
        Undefined_or_default = 15