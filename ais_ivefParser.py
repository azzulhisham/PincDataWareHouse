import xmltodict
import json
import array
import ast

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dataclasses import dataclass, fields
from datetime import datetime,timezone

engine = create_engine('postgresql://postgres:Az@HoePinc0615@localhost/creative', echo=True)
Base = declarative_base()
Session = sessionmaker(bind=engine)


class SwaslaDto(Base):
  __tablename__ = 'swasla_dev'

  Id = Column(String, primary_key=True)
  MsgRefId = Column(String)
  Version = Column(String)

  # position & tracking data
  Altitude = Column(Float)
  EstAccAlt = Column(Float)
  EstAccLat = Column(Float)
  EstAccLong = Column(Float)
  Lat = Column(Float)
  Long = Column(Float)
  COG = Column(Float)
  EstAccSOG = Column(Float)
  EstAccCOG = Column(Float)
  Heading = Column(Float)
  ROT = Column(Float)
  SOG = Column(Float)
  UpdateTime = Column(DateTime)
  TrackStatus = Column(Integer)
  NavStatus = Column(Integer)

  # construction
  HullColor = Column(String)
  HullType = Column(Integer)
  DeadWeight = Column(Float)
  GrossWeight = Column(Float)
  Length = Column(Float)
  LloydsShipType = Column(Integer)
  YearOfBuild = Column(Integer)
  MaxAirDraught = Column(Float)
  MaxDraught = Column(Float)
  MaxPersonsOnBoard = Column(Integer)
  MaxSpeed = Column(Float)
  Width = Column(Float)

  # identifier
  Callsign = Column(String)
  IMO = Column(Integer)
  Name = Column(String)
  FormerName = Column(String)
  Flag = Column(String)
  Owner = Column(String)
  MMSI = Column(Integer)
  LRIT = Column(String)

  # vessel data
  Class = Column(Integer)
  SpecialAttention = Column(String)
  SourceId = Column(String)
  SourceName = Column(String)
  SourceType = Column(Integer)

  # waypoint
  ATA = Column(DateTime)
  ETA = Column(DateTime)
  RTA = Column(DateTime)
  LCode = Column(String)

  # voyageData
  AirDraught = Column(Float)
  CargoTypeIMO = Column(Integer)
  ContactIdentity = Column(String)
  DestCode = Column(String)
  DestName = Column(String)
  DepartCode = Column(String)
  DepartName = Column(String)
  Draught = Column(Float)
  ATD = Column(DateTime)
  ISPSLevel = Column(Float)
  OverSizedLength = Column(Float)
  OverSizedWidth = Column(Float)
  PersonsOnBoard = Column(Integer)
  Pilots = Column(Float)

  STYRIS_INFO_1 = Column(String)
  STYRIS_CREATION_U_DATE = Column(Integer)
  STYRIS_ATTENTION_LEVEL = Column(Integer)
  STYRIS_GENERIC_TYPE = Column(Integer)
  STYRIS_TYPE = Column(Integer)
  STYRIS_CANCEL_FLAG = Column(Boolean)
  STYRIS_NGL = Column(String)
  STYRIS_TRACKING_TYPE = Column(Integer)
  STYRIS_SOURCE_TYPE = Column(Integer)
  STYRIS_FREE_FLAG = Column(Boolean)
  STYRIS_CREATION_DATE = Column(Integer)
  STYRIS_AFFILIATION = Column(Integer)
  STYRIS_NAV_STATUS = Column(Integer)
  STYRIS_COMMENT = Column(String)

  def __init__(self, Id, MsgRefId, Version, Altitude='0.0', EstAccAlt='0.0', EstAccLat='0.0', Lat='0.0', Long='0.0', COG='0.0', EstAccSOG='0.0', EstAccCOG='0.0', Heading='0.0', ROT='0.0', SOG='0.0', UpdateTime=None, TrackStatus='0', NavStatus='0',
               HullColor=None, HullType='0', DeadWeight='0.0', GrossWeight='0.0', Length='0.0', LloydsShipType='0', YearOfBuild='0', MaxAirDraught='0.0', MaxDraught='0.0', MaxPersonsOnBoard='0', MaxSpeed='0.0', Width='0.0',
               Callsign=None, IMO='0', Name=None, FormerName=None, Flag=None, Owner=None, MMSI='0', LRIT=None, Class='0', SpecialAttention=None, SourceId=None, SourceName=None, SourceType='0', ATA=None, ETA=None, RTA=None, LCode=None,
               AirDraught='0.0', CargoTypeIMO='0', ContactIdentity=None, DestCode=None, DestName=None, DepartCode=None, DepartName=None, Draught='0.0', ATD=None, ISPSLevel='0.0', OverSizedLength='0.0', OverSizedWidth='0.0', PersonsOnBoard='0', Pilots='0.0',
               STYRIS_INFO_1=None, STYRIS_CREATION_U_DATE='0', STYRIS_ATTENTION_LEVEL='0', STYRIS_GENERIC_TYPE='0', STYRIS_TYPE='0', STYRIS_NGL=None, STYRIS_CANCEL_FLAG=False, STYRIS_TRACKING_TYPE='0', STYRIS_SOURCE_TYPE='0', 
               STYRIS_FREE_FLAG=False, STYRIS_CREATION_DATE='0', STYRIS_AFFILIATION='0', STYRIS_NAV_STATUS='0', STYRIS_COMMENT=None):

    self.Id = Id
    self.MsgRefId = MsgRefId
    self.Version = Version
    self.Altitude = ast.literal_eval(Altitude)
    self.EstAccAlt = ast.literal_eval(EstAccAlt)
    self.EstAccLat = ast.literal_eval(Id)
    self.EstAccLong = ast.literal_eval(EstAccLat)
    self.Lat = ast.literal_eval(Lat)
    self.Long = ast.literal_eval(Long)
    self.COG = ast.literal_eval(COG)
    self.EstAccSOG = ast.literal_eval(EstAccSOG)
    self.EstAccCOG = ast.literal_eval(EstAccCOG)
    self.Heading = ast.literal_eval(Heading)
    self.ROT = ast.literal_eval(ROT)
    self.SOG = ast.literal_eval(SOG)
    self.UpdateTime = datetime.strptime(UpdateTime, "%Y-%m-%dT%H:%M:%S.%f%z")
    self.TrackStatus = ast.literal_eval(TrackStatus)
    self.NavStatus = ast.literal_eval(NavStatus)

    self.HullColor = HullColor
    self.HullType = ast.literal_eval(HullType)
    self.DeadWeight = ast.literal_eval(DeadWeight)
    self.GrossWeight = ast.literal_eval(GrossWeight)
    self.Length = ast.literal_eval(Length)
    self.LloydsShipType = ast.literal_eval(LloydsShipType)
    self.YearOfBuild = ast.literal_eval(YearOfBuild)
    self.MaxAirDraught = ast.literal_eval(MaxAirDraught)
    self.MaxDraught = ast.literal_eval(MaxDraught)
    self.MaxPersonsOnBoard = ast.literal_eval(MaxPersonsOnBoard)
    self.MaxSpeed = ast.literal_eval(MaxSpeed)
    self.Width = ast.literal_eval(Width) 

    self.Callsign = Callsign
    self.IMO = ast.literal_eval(IMO) 
    self.Name = Name
    self.FormerName = FormerName
    self.Flag: Flag
    self.Owner = Owner
    self.MMSI = MMSI
    self.LRIT = LRIT

    self.Class = ast.literal_eval(Class) 
    self.SpecialAttention = SpecialAttention
    self.SourceId = SourceId
    self.SourceName = SourceName
    self.SourceType = ast.literal_eval(SourceType) 

    self.ATA = datetime.strptime(ATA, "%Y-%m-%dT%H:%M:%S.%f%z") if not ATA == None else None
    self.ETA = datetime.strptime(ETA, "%Y-%m-%dT%H:%M:%S.%f%z") if not ETA == None else None
    self.RTA = datetime.strptime(RTA, "%Y-%m-%dT%H:%M:%S.%f%z") if not RTA == None else None
    self.LCode = LCode

    self.AirDraught = ast.literal_eval(AirDraught) 
    self.CargoTypeIMO = ast.literal_eval(CargoTypeIMO) 
    self.ContactIdentity = ContactIdentity
    self.DestCode = DestCode
    self.DestName = DestName
    self.DepartCode = DepartCode
    self.DepartName = DepartName
    self.Draught = ast.literal_eval(Draught) 
    self.ATD = datetime.strptime(ATD, "%Y-%m-%dT%H:%M:%S.%f%z") if not ATD == None else None
    self.ISPSLevel = ast.literal_eval(ISPSLevel) 
    self.OverSizedLength = ast.literal_eval(OverSizedLength) 
    self.OverSizedWidth = ast.literal_eval(OverSizedWidth) 
    self.PersonsOnBoard = ast.literal_eval(PersonsOnBoard) 
    self.Pilots = ast.literal_eval(Pilots)   

    self.STYRIS_INFO_1 = STYRIS_INFO_1
    self.STYRIS_CREATION_U_DATE = ast.literal_eval(STYRIS_CREATION_U_DATE)
    self.STYRIS_ATTENTION_LEVEL = ast.literal_eval(STYRIS_ATTENTION_LEVEL)
    self.STYRIS_GENERIC_TYPE = ast.literal_eval(STYRIS_GENERIC_TYPE)
    self.STYRIS_TYPE = ast.literal_eval(STYRIS_TYPE)
    self.STYRIS_CANCEL_FLAG = True if STYRIS_CANCEL_FLAG == 'true' else False
    self.STYRIS_NGL = STYRIS_NGL
    self.STYRIS_TRACKING_TYPE = ast.literal_eval(STYRIS_TRACKING_TYPE)
    self.STYRIS_SOURCE_TYPE = ast.literal_eval(STYRIS_SOURCE_TYPE)
    self.STYRIS_FREE_FLAG = True if STYRIS_FREE_FLAG == 'true' else False
    self.STYRIS_CREATION_DATE = ast.literal_eval(STYRIS_CREATION_DATE)
    self.STYRIS_AFFILIATION = ast.literal_eval(STYRIS_AFFILIATION)
    self.STYRIS_NAV_STATUS = ast.literal_eval(STYRIS_NAV_STATUS)
    self.STYRIS_COMMENT = STYRIS_COMMENT


  def __repr__(self):
    return f"<SwaslaDto(Id={self.Id}, MMSI={self.MMSI}, IMO={self.IMO}, Callsign={self.Callsign})>"


# Create the table if it does not exist
Base.metadata.create_all(engine)

# Create a session and insert or update records
session = Session()


swasla_dataclass = dict()
# test = SwaslaDto('123456789', 'b55c3aab-b75e-4e4a-b67a-c0568517a560')

# extract data type from the class
# for x in fields(SwaslaDto):
#   swasla_dataclass[x.name] =  x.type

# d = swasla_dataclass['Id']
# print(d)

# test1 = {'Id': '111111', 'MsgRefId':'123-123-123'}
# test2 = SwaslaDto(**test1)

# print(test2.MsgRefId)



# Read the XML file
with open("/Users/zultan/Downloads/data-1692081259569.xml") as f:
    xml_data = f.read()


# xml_data = """
# <MSG_IVEF xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="http://www.iala-to-be-confirmed.org/XMLSchema/IVEF/0.2.5">

#   <Header MsgRefId="{159fdc7a-b9ff-474e-9c04-b65b368cdedd}" Version="0.2.5" />

#   <Body>

#     <ObjectDatas>

#       <ObjectData>

#         <TrackData COG="254.1" Id="202401022004338694" Length="136" Heading="255" ROT="0" SOG="4.47566666671751" SourceName="CoastWatch" UpdateTime="2024-01-02T23:29:47.563Z" TrackStatus="1" Width="22">

#           <Pos Lat="1.23972166666667" Long="103.94434" />

#           <NavStatus Value="0" />

#         </TrackData>

#         <VesselData Class="1" Id="202401022004338694" SourceName="CoastWatch" SourceType="1" UpdateTime="2024-01-02T23:29:47.563Z" />

#         <VoyageData Id="202401022004338694" CargoTypeIMO="0" SourceName="CoastWatch" SourceType="1" UpdateTime="2024-01-02T23:29:47.563Z" />

#       </ObjectData>

#     </ObjectDatas>

#   </Body>

# </MSG_IVEF>
# """






# Define a function to recursively extract all key-value pairs from a nested dictionary
def extract_data(dictData, result):
    key = ''

    # Loop through each key-value pair in the dictionary
    for k, v in dictData.items():
        if isinstance(v, dict):
            extract_data(v, result)
        elif isinstance(v, list):
            for i in v:
              extract_data(i, result)
        else:
            if not k == '@Key' and not k == '@Value':
              result.append((k, v))
            else:
              if k == '@Key':
                key = v
              elif k == '@Value':
                if key == '':
                  key = 'NavStatus'
                result.append((key, v))


def build_Swasla_Dict(data, msgRefId, version):
  swasla_data = dict()
  swasla_data['MsgRefId'] = msgRefId
  swasla_data['Version'] = version
  
  for k, v in data:
    if not v == None:    
      swasla_data[k.replace('@', '')] =  v

  # print(swasla_data)
  return SwaslaDto(**swasla_data)


# Parse the XML data into a dictionary
dict_data = xmltodict.parse(xml_data)
data_header = dict_data['MSG_IVEF']['Header']
data_body = dict_data['MSG_IVEF']['Body']['ObjectDatas']['ObjectData']


if isinstance(data_body, list):
  # print(data_body)

  for i in data_body:
    result = []
    extract_data(i, result)
    swasla = build_Swasla_Dict(result, data_header['@MsgRefId'].replace('{', '').replace('}', ''), data_header['@Version'])

    # sss = session.query(SwaslaDto).get(swasla['Id'])
    # sss = session.query(SwaslaDto).filter(SwaslaDto.SourceName == s.SourceName, SwaslaDto.MsgRefId == '159fdc7a-b9ff-474e-9c04-b65b368cdedd')
    # print(sss)

    s = session.query(SwaslaDto).get(swasla.Id)

    if s is None:
        session.add(swasla)
    else:
        s.Lat = swasla.Lat
        s.Long = swasla.Long
        s.COG = swasla.COG
        s.Heading = swasla.Heading
        s.ROT = swasla.ROT
        s.SOG = swasla.SOG
        s.UpdateTime = swasla.UpdateTime

  session.commit()    

else:
  result = []
  extract_data(data_body, result)
  swasla = build_Swasla_Dict(result, data_header['@MsgRefId'].replace('{', '').replace('}', ''), data_header['@Version'])







