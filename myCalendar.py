import sys
from flask import Flask
from flask import request
from flask import send_file
from flask_restx import Resource, Api
from flask_restx import fields
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from flask_restx import inputs
from flask_restx import reqparse
import json
import datetime
import requests
import sqlite3
from sqlite3 import Error
import base64
import geopandas as gpd
import re

app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
api = Api(app, 
          version="1.0",
          title="MyCalendar", 
          default="Calendar Management", 
          default_label="Time-managing and scheduling services",
          description=" A calendar API on time-management and scheduling for Australians.\n Written by Van Minh Tran")

suburbs_csv = str(sys.argv[1])
cities_csv = str(sys.argv[2])

states_codes = {
    'NSW': 'NEW SOUTH WALES',
    'NT': 'NORTHERN TERRITORY',
    'QLD': 'QUEENSLAND',
    'SA': 'SOUTH AUSTRALIA',
    'TAS': 'TASMANIA',
    'VIC': 'VICTORIA',
    'WA': 'WESTERN AUSTRALIA'
}

weather_keywords = {
    'clear' : 'clear',
    'pcloudy': 'partial cloudy',
    'mcloudy': 'medium cloudy',
    'cloudy': 'cloudy', 
    'humid': 'humid', 
    'lightrain': 'light rain', 
    'shower': 'shower', 
    'lightsnow': 'light snow', 
    'rainsnow': 'rain and snow',
    'tsrain': 'thunderstorm with rain',
    'rain': 'rain',
    'snow': 'snow', 
    'ts': 'thunderstorm',
}

wind_speeds = {
    1: 'Below 0.3m/s (calm)',
    2: '0.3-3.4m/s (light)',
    3: '3.4-8.0m/s (moderate)',
    4: '8.0-10.8m/s (fresh)',
    5: '10.8-17.2m/s (strong)',
    6: '17.2-24.5m/s (gale)',
    7: '24.5-32.6m/s (storm)',
    8: 'Over 32.6m/s (hurricane)',
    -9999: 'invalid'
}

location_model = api.model('Location', {
    'street': fields.String(description="Street number and street name", example='215B Night Ave'),
    'suburb': fields.String(example='Kensington'),
    'state' : fields.String(example='NSW'),
    'post-code': fields.String(example='2033')
})

event_model = api.model('Event', {
    'name': fields.String(example='birthday party'),
    'date': fields.String(description='Format dd-mm-YYYY', example='01-01-2024'),
    'from': fields.String(description="Format HH:mm", example='16:30'),
    'to': fields.String(description="Format HH:mm", example='19:00'),
    'location': fields.Nested(location_model),
    'description': fields.String(example='Some description'),
})

# depend on daylight saving, this could be 10 or 11. Leave it at 10 for default
SYDNEY_GMT = 10

parser = reqparse.RequestParser()
parser.add_argument('order', default='+id')
parser.add_argument('page', type=int, default=1)
parser.add_argument('size', type=int, default=10)
parser.add_argument('filter', default='id,name')

format_parser = reqparse.RequestParser()
format_parser.add_argument('format', default='json')

weather_parser = reqparse.RequestParser()
weather_parser.add_argument('date')

def connect_db():
    conn = sqlite3.connect('myCalendar.db')
    return conn

def create_events_table ():
    try:
        conn = connect_db()
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                date TEXT NOT NULL,
                from_time TEXT NOT NULL,
                to_time TEXT NOT NULL,
                location TEXT NOT NULL,
                description TEXT NOT NULL,
                last_update TEXT NOT NULL
            );
            '''
        )
        print('Events table created successfully')
        conn.commit()
    except Error as e:
        print(e)
    finally:
        conn.close()

def validate_time(time_string, format):
    res = True
    try:
        res = bool(to_date_time_format(time_string, format))
    except ValueError:
        res = False
        #raise ValueError('Incorrect Date Format')
    return res

def validate_state(state):
    if state in states_codes.keys() or state in states_codes.values():
        return True
    else:
        return False

# date is store as YYYYmmdd for ease of comparison
def date_format(date):
    ret = to_date_time_format(date, '%d-%m-%Y')
    return to_str_time_format(ret, '%Y%m%d')

def insert_event(event):
    try:    
        location = event['location']

        #convert state inside location to the state full name according to states_codes dict
        #for consistent storage and query matching
        location_str = json.dumps(location)
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("INSERT INTO events (name, date, from_time, to_time, location, description, last_update) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (event['name'], event['date'], event['from'], event['to'], location_str, event['description'], event['last-update']))
        conn.commit()
        new_id = cur.lastrowid
    except:
        conn.rollback()
    finally:
        conn.close()
    return new_id

def check_event_overlap(event):
    try:
        date = event['date']
        event_from = to_date_time_format(event['from'], '%H:%M').time()
        event_to = to_date_time_format(event['to'], '%H:%M').time()
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if "id" in event.keys():
            cur.execute("SELECT * FROM events WHERE date = ? AND id != ?", (date, event['id']))
        else:
            cur.execute("SELECT * FROM events WHERE date = ?", (date, ))
        entries = cur.fetchall()
        if entries:
            for entry in entries:
                entry_from = to_date_time_format(entry['from_time'], '%H:%M').time()
                entry_to = to_date_time_format(entry['to_time'], '%H:%M').time()
                if (entry_from <= event_from < entry_to) or (event_from <= entry_from < event_to):
                    #print("overlapped")
                    return True
                else:
                    #print('not overlap')
                    return False
        else:
            return False
    except Error as e:
        print(e)

def get_event_by_id(event_id):
    event = {}
    try:
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM events WHERE id = ?", (event_id,))
        entry = cur.fetchone()
        
        event["id"] = entry["id"]
        event["last-update"] = entry["last_update"]
        event["name"] = entry["name"]
        event["date"] = entry["date"]
        event["from"] = entry["from_time"]
        event["to"] = entry["to_time"]
        event["location"] = json.loads(entry["location"])
        event["description"] = entry["description"]
    except:
        event = {}

    return event

# clean and round geo point in suburb csv to 6 decimal place, personal preference
def geopoint_process(geopoint):
    res = [float(n) for n in geopoint.split(",")]
    res[0] = round(res[0], 6)
    res[1] = round(res[1], 6)
    return tuple(res)

def suburb_dataset_processing(suburbs):
    sub_df = pd.read_csv(suburbs, sep=';')
    sub_df = sub_df.dropna()
    sub_df['Geo Point'] = sub_df['Geo Point'].map(geopoint_process)
    sub_df['Official Name State'] = sub_df['Official Name State'].map(lambda name: name.upper())
    sub_df['Official Name Suburb'] = sub_df['Official Name Suburb'].map(lambda name: name.upper())
    return sub_df

# Both this function and get_cities weather use SYDNEY_GMT = 10 to calculate which timepoint from the weather API to get data from.
# The fact that this assignment occurs before and after daylight saving time and the API only forecast every 3 hours become the complications 
# that make weather forecast cannot be fully accurate since the API might not reflect the change in DST immediately, but the data retrieved 
# should be from either of the two timepoints closest to after an event's "from" time. (It is 'after' based on the assumption that user will 
# only care about the weather occurs after starting time to know if an event will go smoothly)
def get_weather_data(event, dataframe):
    weather_data = {}
    st = event['location']['state'].upper()
    sub = event['location']['suburb'].upper()

    from_time = event['from']
    date_time = event['date'] + ' ' + event['from']
    date = to_date_time_format(date_time, "%Y%m%d %H:%M")
    now = datetime.datetime.now()

    #print(date)
    if date > now and (date - now).days <= 7:
        h, m = event['from'].split(":")
        from_time = int(h) + int(m)/60
        days_diff = ((date-now).total_seconds()/(24 * 60 * 60))
        event_time_point = int(np.ceil(days_diff))*24 + from_time

        res = dataframe.query("`Official Name State` == @st and `Official Name Suburb`.str.contains(@sub)")
        #print(res)
        if len(res.index) == 0: return {}
        geo_point = res.head(1).iloc[0]['Geo Point']
        lat = geo_point[0]
        lon = geo_point[1]
        url = "https://www.7timer.info/bin/api.pl?lon={lon}&lat={lat}&product=civil&output=json".format(lon=lon, lat=lat)
        #print(url)
        response = requests.get(url)
        data = json.loads(response.text)

        data_series = data['dataseries']
        init_hour = int(data['init'][-2:])
        for entry in data_series:
            if entry['timepoint'] >= (np.ceil(event_time_point) - init_hour - SYDNEY_GMT):
                weather_data = entry
                break
    else:
        weather_data = {}
    return weather_data

def get_holiday_data(event):
    date = to_date_time_format(event['date'], "%Y%m%d")
    url = "https://date.nager.at/api/v2/publicholidays/{}/AU".format(date.year)
    response = requests.get(url)
    data = json.loads(response.text)
    date_str = datetime.datetime.strftime(date, "%Y-%m-%d")

    holiday_list = []
    holiday_data = []
    for entry in data:
        if entry['date'] == date_str:
            holiday_list.append(entry)
    
    if len(holiday_list) > 0:
        for h in holiday_list:
            if h['counties']:        
                counties = '(' + ', '.join(map(lambda x:x[3:], h['counties'])) + ')'
                holiday_data.append(h['localName'] + ' ' + counties)
            else:
                 holiday_data.append(h['localName'])
    return holiday_data

def get_event_metadata(event_id):
    # check if holiday
    # weather forecast if in next 7 days
    metadata = {
        "wind-speed": None, 
        "weather": None,
        "humidity": None,
        "temperature": None,
        "holiday": "No",
        "weekend": False
    }
    event = get_event_by_id(event_id)
    date = to_date_time_format(event['date'], "%Y%m%d")
    suburbs_df = suburb_dataset_processing(suburbs_csv)

    weather_data = get_weather_data(event, suburbs_df)
    if weather_data:
        speed_val = weather_data['wind10m']['speed']
        metadata['wind-speed'] = wind_speeds[speed_val]
        for key in weather_keywords.keys():
            if key in weather_data['weather']:
                metadata['weather'] = weather_keywords[key]
        metadata['humidity'] = weather_data['rh2m']
        metadata['temperature'] = str(weather_data['temp2m']) + " C"
    else:
        metadata.pop('wind-speed')
        metadata.pop('weather')
        metadata.pop('humidity')
        metadata.pop('temperature')

    holiday_data = get_holiday_data(event)
    if len(holiday_data) > 0:
        metadata['holiday'] = ', '.join(holiday_data)
    
    event_weekday = date.weekday()
    if event_weekday >= 5:
        metadata['weekend'] = True
    else:
        metadata['weekend'] = False
    

    return metadata

def get_next_event_link(event_id):
    event = get_event_by_id(event_id)
    #print(event)
    date = event['date']
    event_from = event['from']
    conn = connect_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM events WHERE date = ? AND from_time > ? ORDER BY from_time ASC LIMIT 1", (date, event_from))
    entry = cur.fetchone()
    if entry:
        return "events/" + str(entry["id"])
    else:
        cur.execute("SELECT * FROM events WHERE date > ? ORDER BY date ASC, from_time ASC LIMIT 1", (date,))
        entry = cur.fetchone()
        if entry:
            return "events/" + str(entry["id"])
        else:
            return 'None'

def get_previous_event_link(event_id):
    event = get_event_by_id(event_id)
    #print(event)
    date = event['date']
    event_from = event['from']
    conn = connect_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM events WHERE date = ? AND from_time < ? ORDER BY from_time DESC LIMIT 1", (date, event_from))
    entry = cur.fetchone()
    if entry:
        return "events/" + str(entry["id"])
    else:
        cur.execute("SELECT * FROM events WHERE date < ? ORDER BY date DESC, from_time DESC LIMIT 1", (date,))
        entry = cur.fetchone()
        if entry:
            return "events/" + str(entry["id"])
        else:
            return 'None'

def basic_response(event_id):
    response = {
        'id': None,
        "last-update": None,
        "_links": {
            "self": {
              "href": None
            }
        }
    }
    event = get_event_by_id(event_id)
    response['id'] = event['id']
    response['last-update'] = event['last-update']
    response['_links']['self']['href'] = "/events/" + str(event_id)
    return response

def detailed_response(event_id):
    response = {
        'id': None,
        "last-update": None,
        "name": None,
        "date": None,
        "from": None,
        "to": None,
        "location": {
            "street": None,
            "suburb": None,
            "state" : None,
            "post-code": None
        },
        "description" : "some notes on the event",
        "_metadata" : {},
        "_links": {
            "self": {
              "href": None
            },
            "previous": {
              "href": None
            },
            "next": {
              "href": None
            }
        }
    }
    event = get_event_by_id(event_id)
    response["id"] = event["id"]
    response["last-update"] = event["last-update"]
    response["name"] = event["name"]
    event_date = to_date_time_format(event['date'], '%Y%m%d')
    response["date"] = to_str_time_format(event_date, '%d-%m-%Y')
    response["from"] = event["from"]
    response["to"] = event["to"]
    response["location"] = event["location"]
    response["location"]['state'] = [k for k, v in states_codes.items() if v == event["location"]['state']][0]
    response["location"]['suburb'] = event["location"]['suburb'].capitalize()
    response["description"] = event["description"]
    response["_metadata"] = get_event_metadata(event_id)

    response['_links']['self']['href'] = "/events/" + str(event_id)
    if get_previous_event_link(event_id) == 'None':
        response['_links'].pop('previous', None)
    else:
        response['_links']['previous']['href'] = get_previous_event_link(event_id)
    if get_next_event_link(event_id) == 'None':
        response['_links'].pop('next', None)
    else:
        response['_links']['next']['href'] = get_next_event_link(event_id)
    return response

def delete_event(event_id):
    msg = {}
    try:
        conn = connect_db()
        conn.execute("DELETE from events WHERE id = ?", (event_id,))
        conn.commit()
        msg['message'] = "The event with id {} was removed from the database!".format(event_id)
        msg['id'] = event_id
    except:
        conn.rollback()
        msg['message'] = "Cannot delete event {}".format(event_id)
    finally:
        conn.close()
    return msg

def update_event(event):
    updated_event = {}
    try:
        location = event['location']
        location_str = json.dumps(location)

        conn = connect_db()
        cur = conn.cursor()
        cur.execute("UPDATE events SET name = ?, date = ?, from_time = ?, to_time = ?, location = ?, description = ?, last_update = ? WHERE id = ?", 
                (event['name'], event['date'], event['from'], event['to'], location_str, event['description'], event['last-update'], event['id']))
        conn.commit()

        updated_event = get_event_by_id(event['id'])
        #print(update_event)
    except:
        conn.rollback()
        updated_event = {}
    finally:
        conn.close()
    
    return updated_event

def validate_id(event_id):
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM events WHERE id = ?", (event_id,))
        entry = cur.fetchone()
        if entry:
            return True
        else:
            return False
    except Error as e:
        print(e)

#convert datetime to string with given format
def to_str_time_format(t, format):
    #return t.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.datetime.strftime(t, format)

#convert string to datetime type with given format
def to_date_time_format(t, format):
    return datetime.datetime.strptime(t, format)

def get_events(order, filters):
    events = []
    columns = filters
    for i in range(len(columns)):
        if columns[i] == 'from' or columns[i] == 'to':
            columns[i] = columns[i] + '_time'
    columns = ','.join(columns)
    order_by = ','.join(order)
    try:
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = "SELECT {} FROM events ORDER BY {}".format(columns, order_by)
        cur.execute(query)
        entries = cur.fetchall()
        for entry in entries:
            event = {}
            for field in filters:
                if field == 'location':
                    event['location'] = json.loads(entry['location'])
                    event['location']['suburb'] = event['location']['suburb'].capitalize()
                    event['location']['state'] = ' '.join(w.capitalize() for w in event['location']['state'].split())
                elif field == 'date':
                    date_db = to_date_time_format(entry['date'], '%Y%m%d')
                    event[field] = to_str_time_format(date_db, '%d-%m-%Y')
                elif field == 'from_time' or field == 'to_time':
                    event[field[:-5]] = entry[field]
                else:
                    event[field] = entry[field]
            events.append(event)
    except:
        events = []
    
    return events

def list_response(params):
    response = {
        "page": None,
        "page-size": None,
        'events': [],
        "_links": {
            "self": {
              "href": None
            },
            "previous": {
              "href": None
            },
            "next": {
              "href": None
            }
        }
    }
    orders = params['order_by']
    page_num = params['page_num']
    size = params['size']
    filters = params['filters']

    order_by = []
    orders_list = orders.split(',')
    valid_criterias = ['id', 'name', 'datetime']
    if len(orders_list) > 3:
        return {"message": "Invalid order criteria(s). Criteria must be a combindation of {+,-}{id, name, datetime}, comma-separated"}, 400
    for o in orders_list:
        order = o[1:]
        sign = o[0]
        if sign not in ['+', '-']:
            return {"message": "Invalid ordering. Use either '+' or '-' before criteria for ordering"}, 400
        if order not in valid_criterias:
            return {"message": "Invalid order criteria(s). Criteria must be a combindation of {+,-}{id, name, datetime}, comma-separated"}, 400
        elif sign == '+':
            order_by.append(order + ' ' + 'ASC')
        elif sign == '-':
            order_by.append(order + ' ' + 'DESC')

    dt = ''
    date_from = []
    for ord in order_by:
        if 'datetime' in ord:
            dt = ord
            # datetime is a combination of date and from. dt[8:] to get ordering ASC or DESC
            date_from.append('date'+dt[8:])
            date_from.append('from_time'+dt[8:])

    #this replace e.g. 'datetime ASC' with 'date ASC, from_time ASC'
    order_by = [x for d in order_by for x in (date_from if d == dt else [d])]

    if (page_num <= 0) :
        return {"message": "Invalid page number. Page index starts from 1"}, 400

    filters_list = filters.split(',')
    for f in filters_list:
        if f not in ['id','name','date','from','to','location']:
            return {"message": "Invalid filter value(s)"}, 400


    #get events and pagination
    events = get_events(order_by, filters_list)
    page_events = [events[i:i+size] for i in range(0, len(events), size)]
    if page_num > len(page_events):
        return {"message": "Supplied page number is out of range. The current last page is {}".format(len(page_events))}, 404


    response['page'] = page_num
    response['page-size'] = size
    response['events'] = page_events[page_num-1]

    link = "/events?order={}&page={}&size={}&filter={}"
    response['_links']['self']['href'] = link.format(orders, page_num, size, filters)
    response['_links']['previous']['href'] = link.format(orders, page_num-1, size, filters)
    response['_links']['next']['href'] = link.format(orders, page_num+1, size, filters)
    if page_num == 1:
        response['_links'].pop('previous', None)
    elif page_num == len(page_events):
        response['_links'].pop('next', None)
    return response, 200

def get_total():
    total = 0
    try:
        conn = connect_db()
        #conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        total_query = "SELECT COUNT(id) FROM events"
        cur.execute(total_query)
        total = cur.fetchone()[0]
    except:
        total = 0
    return total

def get_total_current_month(current_month_year):
    total_m = 0
    #print(current_month_year)
    try:
        conn = connect_db()
        cur = conn.cursor()
        month_query = "SELECT COUNT(id) FROM events WHERE date LIKE '{cond}'".format(cond = current_month_year + '%')
        cur.execute(month_query)
        total_m = cur.fetchone()[0]
    except:
        total_m = 0
    return total_m

def get_current_year_events(current_year):
    try:
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = "SELECT * FROM events WHERE date LIKE '{cond}'".format(cond = current_year  + '%')
        cur.execute(query)
        entries = cur.fetchall()
    except:
        entries = []
    return entries

def get_total_current_week(today, current_year):
    total_w = 0
    #print(current_month_year)
    entries = get_current_year_events(current_year)
    for entry in entries:
        entry_date = to_date_time_format(entry['date'], '%Y%m%d')
        entry_week = entry_date.isocalendar()[1]
        current_week = today.isocalendar()[1]
        if entry_week == current_week and entry_date.weekday() >= today.weekday():
            total_w += 1
    return total_w

def get_per_day_events():
    res = {}
    try:
        conn = connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = "SELECT date, COUNT(id) FROM events GROUP BY date ORDER BY date"
        cur.execute(query)
        entries = cur.fetchall()
        for entry in entries:
            date = to_date_time_format(entry['date'], '%Y%m%d')
            date = to_str_time_format(date, '%d-%m-%Y')
            res[date] = entry['count(id)']
    except:
        res = {}
    return res

def json_statistics():
    stats = {
        'total': 0,
        'total-current-week': 0, 
        'total-current-month': 0,
        'per-days': {}
    }
    today = datetime.datetime.today()
    today_str = to_str_time_format(today, '%Y%m%d')
    current_year = today_str[:4]
    current_month_year = today_str[:6]
    #current_week = today.isocalendar()[1]
    stats['total'] = get_total()
    stats['total-current-week'] = get_total_current_week(today, current_year)
    stats['total-current-month'] = get_total_current_month(current_month_year)
    stats['per-days'] = get_per_day_events()

    return stats

def visualise_stats():
    stats = json_statistics()
    days = stats['per-days']
    total = int(stats['total'])
    cur_week = int(stats['total-current-week'])
    cur_month = int(stats['total-current-month'])

    today = datetime.datetime.now()
    today_str = to_str_time_format(today, '%d-%m-%Y')
    today_month = to_str_time_format(today, '%B')

    plt.switch_backend( 'Agg' )
    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10,20))
    #gs = fig.add_gridspec(2,2)

    weeks = [cur_week, (total - cur_week)]
    week_labels = 'From today ({}) to end of week'.format(today_str), 'Other weeks'
    axes[0].pie(weeks, labels= week_labels, colors = ['tab:orange', 'tab:green'], radius=1.1, 
                autopct= lambda x: '{:.1f}%\n({v:d})'.format(x, v=int(round(x*total/100.0))), explode = [0.1,0])
    axes[0].set_title('Number of events based on week'.format(day=today_str), fontweight ='bold', fontsize = 15, pad=18)

    months = [cur_month, (total - cur_month)]
    month_labels = 'This month ({})'.format(today_month), 'Other months'
    axes[1].pie(months, labels= month_labels, colors = ['red', 'grey'], radius=1.1, 
                autopct= lambda x: '{:.1f}%\n({v:d})'.format(x, v=int(round(x*total/100.0))), explode = [0.1,0])
    axes[1].set_title('Number of events based on month', fontweight ='bold', fontsize = 15, pad=18)

    for k, v in days.items():
        if k == today_str:
            today_bar = axes[2].bar(k, v, color = 'orange', label='Today')
        else:
            d = to_date_time_format(k, '%d-%m-%Y')
            if d > today:
                future = axes[2].bar(k, v, color = 'tab:cyan', label='Future Events')
            elif d < today:
                past = axes[2].bar(k, v, color = 'tab:grey', label='Past Events')
    axes[2].set_yticks(np.arange(0, max(list(days.values()))+1, 1))
    axes[2].set_title('Number of events per day', fontweight ='bold', fontsize = 15, pad=18)
    axes[2].tick_params(axis='x', rotation=60)
    axes[2].set_xlabel('Dates', fontweight ='bold', fontsize = 12)
    axes[2].set_ylabel('Number of events', fontweight ='bold', fontsize = 12)

    handles, labels = axes[2].get_legend_handles_labels()
    handle_list, label_list = [], []
    for handle, label in zip(handles, labels):
        if label not in label_list:
            handle_list.append(handle)
            label_list.append(label)
    axes[2].legend(handle_list, label_list)

    plt.tight_layout(pad=5.0)
    plt.savefig('image-stats.png')

def cities_dataset_processing(cities):
    cities_df = pd.read_csv(cities)
    cities_list = ['Sydney', 'Melbourne', 'Brisbane', 'Canberra', 'Perth', 'Adelaide', 'Alice Springs', 'Hobart', 'Broome', 'Darwin', 'Cairns']
    cities_df = cities_df.query("`city` in @cities_list").drop(columns=['country', 'iso2'])
    return cities_df.head(11)

def weather_by_city_loc(date, city_lat, city_lon):
    lon = city_lon
    lat = city_lat
    url = "https://www.7timer.info/bin/api.pl?lon={lon}&lat={lat}&product=civil&output=json".format(lon=lon, lat=lat)
    weather_data= {}
    response = requests.get(url)
    data = json.loads(response.text)
    time = '12:00'
    date_time = date + ' ' + time
    date = datetime.datetime.strptime(date_time, "%d-%m-%Y %H:%M")
    now = datetime.datetime.now()
    h, m = time.split(":")
    from_time = int(h) + int(m)/60
    days_diff = ((date-now).total_seconds()/(24 * 60 * 60))
    time_point = int(np.ceil(days_diff))*24 + from_time
    data_series = data['dataseries']
    init_hour = int(data['init'][-2:])
    for entry in data_series:
        if entry['timepoint'] >= (np.ceil(time_point) - init_hour - SYDNEY_GMT):
            weather_data['temperature'] = entry['temp2m']
            weather_data['humidity'] = entry['rh2m']
            weather_data['weather']= entry['weather']
            break
    for key in weather_keywords.keys():
        if key in weather_data['weather']:
            weather_data['weather'] = weather_keywords[key]
    return weather_data

def get_cities_weather(date):
    date_str = date_format(date)
    top_cities = cities_dataset_processing(cities_csv)
    top_cities['weather info'] = top_cities.apply(lambda x: weather_by_city_loc(date, x['lat'], x['lng']), axis=1)

    plt.switch_backend('Agg')
    fig, ax = plt.subplots(figsize=(25,20))
    countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    geo_au = countries[countries["name"] == "Australia"]
    ax = geo_au.plot(color="#A3AB73", ax=ax)

    ax = top_cities.plot.scatter(x="lng", y="lat", s=200, c='orange', ax=ax)
    for idx, row in top_cities.iterrows():
        coord_str = '{}\n{} C | {}\nHumidity: {}'.format(row['city'].upper(), 
                                                    row['weather info']['temperature'], 
                                                    row['weather info']['weather'].title(), 
                                                    row['weather info']['humidity'])
        if row['city'] == 'Sydney' or row['city'] == 'Adelaide':
            ax.annotate(coord_str, (row['lng'], row['lat']), xytext=(15,15), textcoords='offset points', 
                        fontweight='bold', fontsize=20,  fontname='monospace', bbox=dict(boxstyle="square",
                        fc="#FBC847"))
        else:
            ax.annotate(coord_str, (row['lng'], row['lat']), xytext=(22,-27), textcoords='offset points', 
                        fontweight='bold', fontsize=20,  fontname='monospace', bbox=dict(boxstyle="square",
                        fc="#FBC847"))
    ax.set_xlim([105,165])
    #ax.grid(False)
    # ax.axis('off')
    #ax.set_frame_on(False)
    fig.set_facecolor("#00495f")
    ax.set_facecolor("#00495f")
    ax.set_axis_off()
    ax.add_artist(ax.patch)
    ax.patch.set_zorder(-1)

    ax.set_title('WEATHER FORECAST FOR {} (MAIN CITIES)'.format(date), fontname='monospace', fontweight='bold', fontsize=45, backgroundcolor="#FBC847")
    plt.tight_layout()
    plt.savefig("weatherforecast-{}.png".format(date_str))
    #return top_cities

@api.route('/events/<int:id>')
@api.param('id', 'An ID of an event')
class Event(Resource):

    @api.response(404, 'Event with supplied ID does not exists')
    @api.response(200, 'Success. Event with supplied ID has been retrieved')
    @api.doc(description="Get an event by ID, with addtional info to be displayed if the event occurs in the next 7 days.")
    def get(self, id):
        if not validate_id(id):
            return {"message": "Event {} does not exists".format(id)}, 404
        return detailed_response(id), 200
    
    @api.response(404, 'Event with supplied ID does not exists')
    @api.response(200, 'Success. Event with supplied ID has been deleted')
    @api.doc(description="Delete an event by ID.")
    def delete(self, id):
        if not validate_id(id):
            return {"message": "Event {} does not exists".format(id)}, 404
        return delete_event(id), 200

    @api.response(404, 'Event with supplied ID does not exists')
    @api.response(400, 'Validation Error. Refer to the return message for more details')
    @api.response(200, 'Success. Event with supplied ID has been updated. If there were no new change to the event, last-update will not change')
    @api.doc(description="Update an event by ID. Payload may contains part or all of available attributes.")
    @api.expect(event_model, validate=True)
    def patch(self, id):
        if not validate_id(id):
            return {"message": "Event {} does not exists".format(id)}, 404
        new_event = request.json
        event = get_event_by_id(id)
        last_update = to_str_time_format(datetime.datetime.now(), '%d-%m-%Y %H:%M:%S')

        for key in new_event.keys():
            if key not in event_model.keys():
                return {"message": "Property {} is invalid".format(key)}, 400
            if key == 'location':            
                event['location']['street'] = new_event['location']['street']
                event['location']['suburb'] = new_event['location']['suburb'].upper()
                state = new_event['location']['state'].upper()
                #print(state)
                if state in states_codes.keys():
                    event['location']['state'] = states_codes[state]
                else:
                    event['location']['state'] = state.upper()
                event['location']['post-code'] = new_event['location']['post-code']
            elif key == 'date':
                if not (validate_time(new_event['date'], '%d-%m-%Y')):
                    return {"message": "Invalid date input"}, 400
                else:
                    date = date_format(new_event['date'])
                    event['date'] = date
            else:
                event[key] = new_event[key]

        #print(event)
        if not (validate_time(event['from'], '%H:%M')) or not (validate_time(event['to'], '%H:%M')):
            return {"message": "Invalid time input"}, 400
        if to_date_time_format(event['from'], '%H:%M').time() > to_date_time_format(event['to'], '%H:%M').time():
            return {"message": "From time (start) must be smaller than To time (finish)"}, 400
        if not validate_state(event['location']['state']):
            return {"message": "Invalid state input. Please input a state by its code (e.g. NSW) or full name"}, 400
        if check_event_overlap(event):
            return {"message": "Event is overlapped with at least one existing event"}, 400
        event_db = get_event_by_id(id)
        #print(event_db)
        if event == event_db:
            event['last-update'] = event_db['last-update']
        else:
            event['last-update'] = last_update
        update_event(event)
        return basic_response(id), 200

@api.route('/events')
class EventsList(Resource):

    @api.param('filter', 'For each result, specify what attribute(s) will be retrieved and shown')
    @api.param('size', 'Specify page size i.e. number of results per page')
    @api.param('page', 'Specify a page to get, starting from 1')
    @api.param('order', 'Combination of sign {+,-} and an attribute {id, name, datetime} to sort. Sign indicates sorting order: + is ascending and - is descending. Can have multiple values e.g. +name, +id')
    @api.response(400, 'Validation Error. Refer to the return message for more details')
    @api.response(404, 'Page not found. Perhaps the page supplied was out of range')
    @api.response(200, 'Success. A list of events has been retrieved based on criterias')
    @api.doc(description="Get a list of events based on criterias. See description of each criteria for more details.")
    @api.expect(parser)
    def get(self):
        args = parser.parse_args()

        orders = args.get('order', '+id').replace(' ','')
        page_num = args.get('page', 1)
        size = args.get('size', 10)
        filters = args.get('filter', 'id,name').replace(' ','')
        params = {
            'order_by': orders,
            'page_num': page_num,
            'size': size,
            'filters': filters
        }
        msg = list_response(params)
        return msg

    @api.response(400, 'Validation Error. Refer to the return message for more details')
    @api.response(201, 'New event created successfully')
    @api.doc(description="Create a new event. See example payload for what attributes are required.")
    @api.expect(event_model, validate=True)
    def post(self):
        event = request.json
        state = event['location']['state'].upper()
        # today = datetime.datetime.now()
        # date_time = event['date'] + ' ' + event['from']
        if state in states_codes.keys():
            event['location']['state'] = states_codes[state]
        else:
            event['location']['state'] = state.upper()
        if len(event.keys()) != len(event_model.keys()):
            return {"message": "Missing required field(s) {}".format(set(event.keys()) ^ set(event_model.keys()))}, 400

        if not (validate_time(event['date'], '%d-%m-%Y')):
            return {"message": "Invalid date input"}, 400
        else:
            date = date_format(event['date'])
            event['date'] = date
        if not (validate_time(event['from'], '%H:%M')) or not (validate_time(event['to'], '%H:%M')):
            return {"message": "Invalid time input"}, 400
        if to_date_time_format(event['from'], '%H:%M').time() > to_date_time_format(event['to'], '%H:%M').time():
            return {"message": "From time (start) must be smaller than To time (finish)"}, 400

        # if to_date_time_format(date_time, '%d-%m-%Y %H:%M') < today:
        #     return {"message": "Cannot schedule event in the past"}, 400

        if not validate_state(event['location']['state']):
            return {"message": "Invalid state input. Please input a state by its code (e.g. NSW) or full name"}, 400
        if check_event_overlap(event):
            return {"message": "Event is overlapped with at least one existing event"}, 400

        for key in event:
            if key not in event_model.keys():
                return {"message": "Property {} is invalid".format(key)}, 400
        last_update = to_str_time_format(datetime.datetime.now(), '%d-%m-%Y %H:%M:%S')
        event['last-update'] = last_update
        event['location']['suburb'] = event['location']['suburb'].upper()
        new_id = insert_event(event)
        return basic_response(new_id), 201

@api.route('/events/statistics')
class EventsStats(Resource):
    @api.param('format', "Specify a format to get different type of statistics. Available format: 'json', 'image'")
    @api.response(400, 'Validation Error. Refer to the return message for more details')
    @api.response(200, 'Success')
    @api.doc(description="Get the statistics of existing Events based on format.")
    @api.expect(format_parser)
    def get(self):
        arg = format_parser.parse_args()
        request_format = arg.get('format')
        stats = {}
        if request_format not in ['json', 'image']:
            return {"message": "Invalid format. Requested format must be of type json or image"}, 400
        elif request_format == 'json':
            stats = json_statistics()
            return stats, 200
        elif request_format == 'image':
            visualise_stats()
            # with open("image-stats.png", "rb") as image_file:
            #     encoded_file= base64.b64encode(image_file.read())
            # send_image = encoded_file.decode()
            # return send_image
            return send_file('image-stats.png', mimetype='image/png')

@api.route('/weather')
class Weather(Resource):

    @api.param('date', "Specify a date to get weather forecast for Australia's main cities. Format 'dd-mm-YYYY'")
    @api.response(400, 'Validation Error. Refer to the return message for more details')
    @api.response(200, 'Success')
    @api.doc(description="Get an image of weather forecast for main cities of Australia with specified date (within 7 days).")
    @api.expect(weather_parser)
    def get(self):
        arg = weather_parser.parse_args()
        date = arg.get('date')
        date_str = date_format(date)
        today = datetime.datetime.now()

        if not date:
            return {"message": "A date must be specified"}, 400
        elif not validate_time(date, '%d-%m-%Y'):
            return {"message": "Invalid date input. Date format must be 'dd-MM-YYYY'"}, 400
        elif to_date_time_format(date, '%d-%m-%Y') < today:
            return {"message": "Weather forecast only works with future date"}, 400
        elif (to_date_time_format(date, '%d-%m-%Y') - today).days > 7:
            return {"message": "Date is too far in the future. Weather forecast works with date within 7 days from now"}, 400
        else:
            get_cities_weather(date)
            return send_file('weatherforecast-{}.png'.format(date_str), mimetype='image/png')


if __name__ == '__main__':
    create_events_table()
    app.run(debug=True)