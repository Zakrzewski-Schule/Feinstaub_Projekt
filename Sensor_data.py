import urllib.request as urllib2
from datetime import datetime
from csv import DictReader


class Sensor_Data:
  def __init__(self):
    self.sensor_id = ''
    self.sensor_type = ''
    self.location = 0
    self.lat = 0.0
    self.lon = 0.0
    self.timestamp = datetime.min

  def assign(self, csv):
    self.sensor_id = csv['sensor_id']
    self.sensor_type = csv['sensor_type']
    self.location = int(csv['location'])
    self.lat = float(csv['lat'])
    self.lon = float(csv['lon'])
    self.timestamp = datetime.fromisoformat(csv['timestamp'])
    self.temperature = float(csv['temperature'])
    self.humidity = float(csv['humidity'])

class Sensor_Feinstaub_Data(Sensor_Data):
  def __init__(self):
    super().__init__()
    self.P1 = 0.0
    self.durP1 = 0.0
    self.ratioP1 = 0.0
    self.P2 = 0.0
    self.durP2 = 0.0
    self.ratioP2 = 0.0

  def assign(self, csv):
    super().assign(csv)
    self.P1 = float(csv['P1'])
    self.durP1 = float(csv['durP1'])
    self.ratioP1 = float(csv['ratioP1'])
    self.P2 = float(csv['P2'])
    self.durP2 = float(csv['durP2'])
    self.ratioP2 = float(csv['ratioP2'])
    return self

class Sensor_Temperature_Data(Sensor_Data):
  def __init__(self):
    super().__init__()
    self.temperature = 0.0
    self.humidity = 0.0

  def assign(self, csv):
    super().assign(csv)
    self.sensor_id = csv['sensor_id']
    self.sensor_type = csv['sensor_type']
    self.location = int(csv['location'])
    self.lat = float(csv['lat'])
    self.lon = float(csv['lon'])
    self.timestamp = datetime.fromisoformat(csv['timestamp'])
    self.temperature = float(csv['temperature'])
    self.humidity = float(csv['humidity'])
    return self
  

class Sensor_Temperature_Data_List(list[Sensor_Temperature_Data]):
  def __init__(self, iterable):
    self.min = 0.0
    self.max = 0.0
    self.avg = 0.0
    self.diff = 0.0
    self.min_index = 0
    self.max_index = 0
    super().__init__(item for item in iterable)

  def __setitem__(self, index, item):
      super().__setitem__(index, item)
  
  def assign_calculated(self):
    temps = list(map(lambda x: x.temperature, self))
    self.min = min(temps)
    self.max = max(temps)
    self.avg = (sum(temps) / len(temps))
    self.diff = (max(temps) - min(temps))
    self.min_index = temps.index(self.min)
    self.max_index = temps.index(self.max)



class Sensor_Feinstaub_Data_List(list[Sensor_Feinstaub_Data]):
  def __init__(self, iterable):
    self.P1min = 0.0
    self.P1max = 0.0
    self.P1avg = 0.0
    self.P1diff = 0.0
    self.P1min_index = 0
    self.P1max_index = 0
    self.P2min = 0.0
    self.P2max = 0.0
    self.P2avg = 0.0
    self.P2diff = 0.0
    self.P2min_index = 0
    self.P2max_index = 0
    super().__init__(item for item in iterable)

  def __setitem__(self, index, item):
      super().__setitem__(index, item)
  
  def assign_calculated(self):
    P1s = list(map(lambda x: x.P1, self))
    self.P1min = min(P1s)
    self.P1max = min(P1s)
    self.P1avg = (sum(P1s) / len(P1s))
    self.P1diff = (max(P1s) - min(P1s))
    self.P1min_index = P1s.index(self.P1min)
    self.P1max_index = P1s.index(self.P1max)

    P2s = list(map(lambda x: x.P2, self))
    self.P1min = min(P2s)
    self.P1max = min(P2s)
    self.P1avg = (sum(P2s) / len(P2s))
    self.P1diff = (max(P2s) - min(P2s))
    self.P1min_index = P2s.index(self.P2min)
    self.P1max_index = P2s.index(self.P2max)






class Sensor_Data_old:
  def __init__(self):
    self.sensor_id = ''
    self.sensor_type = ''
    self.location = 0
    self.lat = 0.0
    self.lon = 0.0
    self.timestamp = datetime.min
    self.temperature = 0.0
    self.humidity = 0.0

  def assign(self, csv):
    self.sensor_id = csv['sensor_id']
    self.sensor_type = csv['sensor_type']
    self.location = int(csv['location'])
    self.lat = float(csv['lat'])
    self.lon = float(csv['lon'])
    self.timestamp = datetime.fromisoformat(csv['timestamp'])
    self.temperature = float(csv['temperature'])
    self.humidity = float(csv['humidity'])
    return self
  
class Sensor_Data_List(list[Sensor_Data]):
  def __init__(self, iterable):
    self.min = 0.0
    self.max = 0.0
    self.avg = 0.0
    self.diff = 0.0
    self.min_index = 0
    self.max_index = 0
    super().__init__(item for item in iterable)

  def __setitem__(self, index, item):
      super().__setitem__(index, item)
  
  def assign_calculated(self):
    temps = list(map(lambda x: x.temperature, self))
    self.min = min(temps)
    self.max = max(temps)
    self.avg = (sum(temps) / len(temps))
    self.diff = (max(temps) - min(temps))
    self.min_index = temps.index(self.min)
    self.max_index = temps.index(self.max)