import os
import gzip
import shutil
import urllib.request as urllib2
from datetime import datetime
from csv import DictReader
from tkinter import *
from tkinter import filedialog as fd
import sqlite3
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from matplotlib.lines import Line2D, lineStyles
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk) 
from Sensor_data import (Sensor_Data, Sensor_Temperature_Data, Sensor_Feinstaub_Data, Sensor_Data_List, Sensor_Temperature_Data_List, Sensor_Feinstaub_Data_List)


def cls():
    os.system('cls' if os.name=='nt' else 'clear')

cls()

class HeadRequest(urllib2.Request):
  def get_method(self):
    return 'HEAD'
  

days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]
is_leap_year = lambda year : (year % 400 == 0 and year % 100 == 0) or (year % 4 == 0 and year % 100 != 0)
get_days_in_month = lambda month,year : (29 if is_leap_year(year) else 28) if month == 2 else days_in_months[month - 1]

def generate_download_urls(a_sensor_type, a_sensor_id, a_start, a_end):
  url_list = []
  if len(a_start) < 2 or len(a_end) < 1: return []
  if len(a_end) < len(a_start):
    if len(a_start) == 2: a_end.append(a_start[1])
    if len(a_start) == 3: 
      if len(a_end) >= 1: a_end.append(a_start[2]) 
      if len(a_end) >= 2: a_end.insert(0, get_days_in_month(a_end[0], a_end[1]))
    else:
      a_start.insert(0, 1)
      a_end.insert(0, get_days_in_month(a_end[0], a_end[1]))

  # do not load data from the future
  start_date = datetime(a_start[2],a_start[1], a_start[0])
  ts_today = datetime.today().timestamp()
  ts_end = datetime(a_end[2],a_end[1], a_end[0]).timestamp()
  end_date = datetime.fromtimestamp(min(ts_today, ts_end))

  for year in range(start_date.year, end_date.year + 1):
    months_min = 1 if year > start_date.year else start_date.month
    months_max = 12 if year < end_date.year else end_date.month
    for month in range(months_min, months_max + 1):
      days_min = start_date.day
      days_max = get_days_in_month(month,year)
      if year >= end_date.year and month >= end_date.month:
        days_min = start_date.day
        days_max = end_date.day

      for day in range(days_min, days_max + 1):
        count_before = len(url_list)
        folder_url = f'http://archive.sensor.community/{f"{year}/" if year != datetime.today().year else ""}{year}-{month:02}-{day:02}'
        file_url = f'{year}-{month:02}-{day:02}_{a_sensor_type.lower()}_sensor_{a_sensor_id}'
        
        if (not check_url_exists(folder_url)):
          folder_url = f'http://archive.sensor.community/{year}/{year}-{month:02}-{day:02}'
        if not (check_url_exists(folder_url)):
          folder_url = f'http://archive.sensor.community/{year}-{month:02}-{day:02}'
        
        if (check_url_exists(f'{folder_url}/{file_url}.csv')):
          url_list.append(f'{folder_url}/{file_url}.csv')  
        elif (check_url_exists(f'{folder_url}/{file_url}.csv.gz')):
          url_list.append(f'{folder_url}/{file_url}.csv.gz')        
      
        if (count_before == len(url_list)): # does not exist
          continue
                
  return url_list

def extract_file(datei_name):
  with gzip.open(datei_name, 'rb') as f_in:
    with open(datei_name[0:-3], 'wb') as f_out:
      shutil.copyfileobj(f_in, f_out)
  os.remove(datei_name)   # delete archive after extracting

def check_url_exists(url):
  opener = urllib2.OpenerDirector()
  opener.add_handler(urllib2.HTTPHandler())
  opener.add_handler(urllib2.HTTPDefaultErrorHandler())
  try:
    res = opener.open(HeadRequest(url))

  except urllib2.HTTPError as res:
    pass
  res.close()
  return res.code in [200, 301]

def download_file(url):
  file_name  = url.split("/")[-1]
  file_ext = file_name.split(".")[-1]
  (r_str, _) = urllib2.urlretrieve(url, file_name)
  if (file_ext == "gz"):
    extract_file(r_str)
    file_name = r_str[0:-3]
  return file_name

#print(generate_download_urls('dht22', 3660, [26, 4, 2021], [26, 4, 2021]))



def insert_csv_into_db(csv_lines : Sensor_Data_List):
  conn = sqlite3.connect('sensordaten.db')
  cursor = conn.cursor()

  if (isinstance(csv_lines[0], Sensor_Temperature_Data)):
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS temperaturdaten (sensordaten_id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id INTEGER, 
                      sensor_type TEXT, location INTEGER, lat REAL, lon REAL, timestamp DATETIME, temperature REAL, humidity REAL);''')    
    for line in Sensor_Temperature_Data_List(csv_lines):
      cursor.execute(f'''INSERT INTO temperaturdaten (sensor_id, sensor_type, location, lat, lon, timestamp, temperature, humidity) VALUES
                      ({line.sensor_id}, \'{line.sensor_type}\', {line.location}, {line.lat}, {line.lon}, \'{line.timestamp}\', {line.temperature}, {line.humidity})''')
  else:
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS feinstaubdaten (sensordaten_id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id INTEGER, 
                      sensor_type TEXT, location INTEGER, lat REAL, lon REAL, timestamp DATETIME, P1 REAL, durP1 REAL, ratioP1 REAL, P2 REAL, durP2 REAL, ratioP2 REAL);''')    
    for line in Sensor_Feinstaub_Data_List(csv_lines):
      cursor.execute(f'''INSERT INTO feinstaubdaten (sensor_id, sensor_type, location, lat, lon, timestamp, P1, durP1, ratioP1, P2, durP2, ratioP2) VALUES
                      ({line.sensor_id}, \'{line.sensor_type}\', {line.location}, {line.lat}, {line.lon}, \'{line.timestamp}\', {line.P1}, {line.durP1}, {line.ratioP1}, {line.P2}, {line.durP2}, {line.ratioP2})''')
  

  conn.commit()
  conn.close()

def fetch_csv_from_db(csv_lines : Sensor_Data_List):
    table_name = 'temperaturdaten' if isinstance(csv_lines[0], Sensor_Temperature_Data) else 'feinstaubdaten'
    condition = ''
    if csv_lines != []:
      csv_line = csv_lines[0]
      condition = f''' WHERE sd.sensor_id == {csv_line.sensor_id} 
                      AND sd.sensor_type == \'{csv_line.sensor_type}\' 
                      AND DATE(sd.timestamp) LIKE \'{csv_line.timestamp.date()}\''''
      
    conn = sqlite3.connect('sensordaten.db')
    cursor = conn.cursor()
    # check if table exists
    listOfTables = cursor.execute(f'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'{table_name}\';').fetchall()
    if listOfTables != []:
      cursor.execute(f'SELECT * FROM {table_name} sd{condition};')
      if (isinstance(csv_lines[0], Sensor_Temperature_Data)):
        csv_lines = Sensor_Temperature_Data_List(cursor.fetchall())
      else:
        csv_lines = Sensor_Feinstaub_Data_List(cursor.fetchall())
      conn.close() 
      return True
    else:
      conn.close()
      return False

def read_csv(file_name : str):
  data = DictReader(open(file_name), delimiter=";")
  print(f'{data.line_num:>5}: {data.fieldnames}\n')

  if (list(data.fieldnames or []).count("temperature") > 0): 
    for line in data:
      yield Sensor_Temperature_Data().assign(line)
  else:
    for line in data:
      yield Sensor_Feinstaub_Data().assign(line)


def give_csv_data(file_name) -> Sensor_Data_List:
  data_list = Sensor_Data_List(read_csv(file_name))
  # check if already exist in db
  if not fetch_csv_from_db(data_list):
    insert_csv_into_db(data_list)
  data_list.assign_calculated()
  return Sensor_Data_List(data_list)



# Erzeugung des Fensters
tkFenster = Tk()
tkFenster.title('Feinstaubprojekt')
tkFenster.geometry('650x550')

# Funktion für den Button, die eine CSV Datei runterlädt
def button_download_pressed():
  start_str = txtDateStart.get('1.0', END+"-1c")
  (dt_s_d, dt_s_m, dt_s_y) = start_str.split('.')
  end_str = txtDateEnd.get('1.0', END+"-1c")
  (dt_e_d, dt_e_m, dt_e_y) = end_str.split('.')

  use_temperature_sensor = False
  if (use_temperature_sensor):
    # create url list
    csv_list = Sensor_Temperature_Data_List([])
    url_list = generate_download_urls('dht22', 3705, [int(dt_s_d), int(dt_s_m), int(dt_s_y)], [int(dt_e_d), int(dt_e_m), int(dt_e_y)])
    for url in url_list:
      file_name = download_file(url)
      csv_list = csv_list + give_csv_data(file_name)
    display_temps(csv_list,f"[from {len(url_list)} files]")
  else:
    csv_list = Sensor_Feinstaub_Data_List([])
    url_list = generate_download_urls('sds011', 13660, [int(dt_s_d), int(dt_s_m), int(dt_s_y)], [int(dt_e_d), int(dt_e_m), int(dt_e_y)])
    for url in url_list:
      file_name = download_file(url)
      csv_list = csv_list + give_csv_data(file_name)
    display_feinstaub(csv_list,f"[from {len(url_list)} files]")



# Funktion für den Button, die eine Datei öffnet und den Dateinamen in das Label schreibt
def button_pressed():
  file = fd.askopenfilename()
  temps = give_csv_data(file)
  display_temps(temps, file)



def display_feinstaub(fstaub : Sensor_Feinstaub_Data_List,a_text=""):
  lblFilename.configure(text=a_text)
  lblCSV.configure(text= f'''Höchstwert P1:    {fstaub.P1max:>6}°c
Mindestwert P1:   {fstaub.P1min:>6}°c
Durchschnitt P1:   {fstaub.P1avg:>6.2f}°c
Differenz P1:         {fstaub.P1diff:>6.2f}°c

Höchstwert P2:    {fstaub.P2max:>6}°c
Mindestwert P2:   {fstaub.P2min:>6}°c
Durchschnitt P2:   {fstaub.P2avg:>6.2f}°c
Differenz P2:         {fstaub.P2diff:>6.2f}°c''')
  datumStart = fstaub[0].timestamp.strftime('%d.%m.%Y')
  datumEnd = fstaub[-1].timestamp.strftime('%d.%m.%Y')

  fig = Figure(figsize = (5, 5), dpi=90)
  plot1 = fig.add_subplot()
  plot1.set_xticklabels(list(map(lambda t: t.timestamp.strftime('%H:%M'), fstaub)))
  plot1.set_ylabel('Feinstaubwerte in μg/m³')
  if datumStart != datumEnd:
    plot1.set_title(f'Feinstaubwerte im Zeitraum von {datumStart} bis {datumEnd}')
  else:
    plot1.set_title(f'Feinstaubwerte am {datumStart}')

  max_w = len(fstaub)
  mid = max_w / 2
  p1_max_x = fstaub.P1max_index
  p1_max_y = fstaub.P1max
  p1_min_x = fstaub.P1min_index
  p1_min_y = fstaub.P1min
  p2_max_x = fstaub.P2max_index
  p2_max_y = fstaub.P2max
  p2_min_x = fstaub.P2min_index
  p2_min_y = fstaub.P2min

  show_extremes = True
  if (show_extremes):
    plot1.plot(p1_min_x,p1_min_y,'o')
    plot1.plot(p1_max_x,p1_max_y,'o')
    plot1.plot(p2_min_x,p2_min_y,'o')
    plot1.plot(p2_max_x,p2_max_y,'o')

  if p1_max_x > mid: 
    plot1.hlines(y=p1_max_y, xmin=0, xmax=p1_max_x, linestyles='dashed')
    plot1.text(0, p1_max_y+0.1, f'{fstaub.P1max}°c', style='italic')
  else:
    plot1.hlines(y=p1_max_y, xmin=p1_max_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, p1_max_y+0.1, f'{fstaub.P1max}°c', style='italic')
  if p1_min_x > mid: 
    plot1.hlines(y=p1_min_y, xmin=0, xmax=p1_min_x, linestyles='dashed')
    plot1.text(0, p1_min_y+0.1, f'{fstaub.P1min}°c', style='italic')
  else:
    plot1.hlines(y=p1_min_y, xmin=p1_min_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, p1_min_y+0.1, f'{fstaub.P1min}°c', style='italic')
  if p2_max_x > mid: 
    plot1.hlines(y=p2_max_y, xmin=0, xmax=p2_max_x, linestyles='dashed')
    plot1.text(0, p2_max_y+0.1, f'{fstaub.P2max}°c', style='italic')
  else:
    plot1.hlines(y=p2_max_y, xmin=p2_max_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, p2_max_y+0.1, f'{fstaub.P2max}°c', style='italic')
  if p2_min_x > mid: 
    plot1.hlines(y=p2_min_y, xmin=0, xmax=p2_min_x, linestyles='dashed')
    plot1.text(0, p2_min_y+0.1, f'{fstaub.P2min}°c', style='italic')
  else:
    plot1.hlines(y=p2_min_y, xmin=p2_min_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, p2_min_y+0.1, f'{fstaub.P2min}°c', style='italic')

  plot1.plot(list(map(lambda x: x.P1, fstaub)))
  plot1.plot(list(map(lambda x: x.P2, fstaub)))

  # creating the Tkinter canvas containing the Matplotlib figure 
  canvas = FigureCanvasTkAgg(fig, master=tkFenster) 
  canvas.draw() 
  canvas.get_tk_widget().pack() 

  # creating the Matplotlib toolbar 
  toolbar = NavigationToolbar2Tk(canvas, tkFenster) 
  toolbar.update() 
  canvas.get_tk_widget().pack(side='bottom', after=toolbar,anchor='e',padx=5)



def display_temps(temps : Sensor_Temperature_Data_List,a_text=""):
  lblFilename.configure(text=a_text)
  lblCSV.configure(text= f'''Höchsttemp.:    {temps.max:>6}°c
Mindesttemp.:   {temps.min:>6}°c
Durchschnitt:   {temps.avg:>6.2f}°c
Differenz:         {temps.diff:>6.2f}°c''')
  datumStart = temps[0].timestamp.strftime('%d.%m.%Y')
  datumEnd = temps[-1].timestamp.strftime('%d.%m.%Y')

  fig = Figure(figsize = (5, 5), dpi=90)
  plot1 = fig.add_subplot()
  plot1.set_xticklabels(list(map(lambda t: t.timestamp.strftime('%H:%M'), temps)))
  plot1.set_ylabel('Temperatur in °c')
  if datumStart != datumEnd:
    plot1.set_title(f'Temperaturwerte im Zeitraum von {datumStart} bis {datumEnd}')
  else:
    plot1.set_title(f'Temperaturwerte am {datumStart}')

  max_w = len(fstaub)
  mid = max_w / 2
  max_x = temps.max_index
  max_y = temps.max
  min_x = temps.min_index
  min_y = temps.min

  show_extremes = True
  if (show_extremes):
    plot1.plot(min_x,min_y,'o')
    plot1.plot(max_x,max_y,'o')

  if max_x > mid: 
    plot1.hlines(y=max_y, xmin=0, xmax=max_x, linestyles='dashed')
    plot1.text(0, max_y+0.1, f'{temps.max}°c', style='italic')
  else:
    plot1.hlines(y=max_y, xmin=max_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, max_y+0.1, f'{temps.max}°c', style='italic')
  if min_x > mid: 
    plot1.hlines(y=min_y, xmin=0, xmax=min_x, linestyles='dashed')
    plot1.text(0, min_y+0.1, f'{temps.min}°c', style='italic')
  else:
    plot1.hlines(y=min_y, xmin=min_x, xmax=max_w, linestyles='dashed')
    plot1.text(520, min_y+0.1, f'{temps.min}°c', style='italic')

  plot1.plot(list(map(lambda x: x.temperature, temps)))

  # creating the Tkinter canvas containing the Matplotlib figure 
  canvas = FigureCanvasTkAgg(fig, master=tkFenster) 
  canvas.draw() 
  canvas.get_tk_widget().pack() 

  # creating the Matplotlib toolbar 
  toolbar = NavigationToolbar2Tk(canvas, tkFenster) 
  toolbar.update() 
  canvas.get_tk_widget().pack(side='bottom', after=toolbar,anchor='e',padx=5)



# Label für die Anzeige von Text
lblFilename = Label(master=tkFenster, text='keine Datei ausgewählt')
lblFilename.place(x=5, y=5, height=20)

lblCSV = Label(master=tkFenster, text='', justify='left')
lblCSV.place(x=5, y=150)

# Button der die Funktion button_pressed() aufruft
btnOpenFile = Button(master=tkFenster, text='Datei auswählen', command=button_pressed)
btnOpenFile.place(x=5, y=30, height=20)

lblDateStart = Label(master=tkFenster, text='Startdatum:', justify='left')
lblDateStart.place(x=5, y = 50)
lblDateEnd = Label(master=tkFenster, text='Enddatum:', justify='left')
lblDateEnd.place(x=5, y = 70)

txtDateStart = Text()
txtDateStart.place(x=110, y=50, width=100,height=20)
txtDateEnd = Text()
txtDateEnd.place(x=110, y=70, width=100,height=20)

btnDownloadCSV = Button(master=tkFenster, text='CSV runterladen', command=button_download_pressed)
btnDownloadCSV.place(x=5, y=90, height=20)

# Aktivierung des Fensters
tkFenster.mainloop()
