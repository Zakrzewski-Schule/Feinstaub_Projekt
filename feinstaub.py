import os
import gzip
import shutil
import urllib.request as urllib2
import pandas as pd
import numpy as np
import matplotlib as mat
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime
from csv import DictReader
#from tkinter import Tk, Label, Button, filedialog as fd
from tkinter import *
from tkinter import ttk
from tkinter import filedialog as fd
import matplotlib.pyplot as plt


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
    with open(datei_name[0,3], 'wb') as f_out:
      shutil.copyfileobj(f_in, f_out)

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

#print(generate_download_urls('dht22', 3660, [26, 4, 2021], [26, 4, 2021]))









def read_csv(file_name):
  data = DictReader(open(file_name), delimiter=";")
  print(f'{data.line_num:>5}: {data.fieldnames}\n')

  for line in data:
    print(f"{(data.line_num-1):>5}: {line['timestamp']} {line['temperature']:>6}°c")
    yield float(line['temperature'])

def give_csv_data(file_name):
  temps = list(read_csv(file_name))
  return {'max': max(temps), 'min': min(temps), 'avg': (sum(temps) / len(temps)), 'diff': (max(temps) - min(temps))}


#temps = give_csv_data("2021-04-26_dht22_sensor_3660.csv")
#print(f"max:  {temps['max']:>6}°c\nmin:  {temps['min']:>6}°c\navg:  {temps['avg']:>6.2f}°c\ndiff: {temps['diff']:>6.2f}°c")

def listSelected(event):
  print(lbox.get(lbox.curselection()))

# Erzeugung des Fensters
tkFenster = Tk()
content = ttk.Frame(tkFenster)
frame = ttk.Frame(content)
tkFenster.title('Feinstaub Projekt')
tkFenster.geometry()

lboxvar = StringVar(value=["test1","test2","test3"])
lbox = Listbox(tkFenster,listvariable=lboxvar,height=10,selectmode="extended")

lbox.bind('<<ListboxSelect>>',listSelected)

#csv = pd.read_csv('')

bar = pd.DataFrame({'length': [1.5,0.5,1.2,0,93],
                    'width': [0.7,0.2,0.15,0.2,1.1]},
                    index=['pig','rabbit','duck','chicken','horse'])

plot = bar.plot(kind="bar", title="Feinstaub",figsize=(3,3)).get_figure();

canvas = FigureCanvasTkAgg(plot, tkFenster)

canvas.get_tk_widget().grid(row=0, column=1)

lbox.grid(row=0, column=0)

# Aktivierung des Fensters
tkFenster.mainloop()
