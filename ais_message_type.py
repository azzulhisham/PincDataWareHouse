from enum import Enum

class MessageType(Enum):
    Position_Report_Class_A = 1
    Assigned_schedule_Position_Report_Class_A = 2
    Response_to_interrogation_Position_Report_Class_A = 3
    Base_Station_Report = 4
    Static_and_Voyage_Related_Data = 5
    Binary_Addressed_Message =6 
    Binary_Acknowledge = 7
    Binary_Broadcast_Message = 8
    Standard_SAR_Aircraft_Position_Report = 9
    UTC_and_Date_Inquiry = 10
    UTC_and_Date_Response = 11
    Addressed_Safety_Related_Message = 12
    Safety_Related_Acknowledgement = 13
    Safety_Related_Broadcast_Message = 14
    Interrogation = 15
    Assignment_Mode_Command = 16
    DGNSS_Binary_Broadcast_Message = 17
    Standard_Class_B_CS_Position_Report = 18
    Extended_Class_B_Equipment_Position_Report = 19
    Data_Link_Management = 20
    Aid_to_Navigation_Report = 21
    Channel_Management = 22
    Group_Assignment_Command = 23
    Static_Data_Report = 24
    Single_Slot_Binary_Message = 25
    Multiple_Slot_Binary_Message_With_Communications_State = 26
    Position_Report_For_Long_Range_Applications = 27
