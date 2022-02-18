# Octopus Energy data retrieval and logging

# usage octo.py
#
# - One method to initialise the whole thing. init database.db
# - One method that runs daily to pull data, store and calculate. update
# - One method to back up the database to a given location. backup location
# -
#

# This is a project to pull and analyse data from Octopus Energy
import sys
import logging
from dateutil import parser
from datetime import datetime
import json
import requests
import sqlite3
from requests.auth import HTTPBasicAuth
import shutil

# retrieve the API key and meter details.

from meter_data import Account_Number
from meter_data import API_Key
from meter_data import meterPoint
from meter_data import meterSerial

# This needs can be tweaked to increase the period from and to.
# This could be tweaked to have the user name the table to hold the data.
def pull_data_from_octopus(database, meter_point, meter_serial, api_key):
    # add sanitation for file name
    conn = sqlite3.connect(database)
    c = conn.cursor()
    connection_string = "https://api.octopus.energy/v1/electricity-meter-points/" + meter_point + "/meters/" + meter_serial + "/consumption/"
    while True:
        res = requests.get(connection_string, verify=True, auth=HTTPBasicAuth(api_key, ''))

        json_data = res.json()
        number_points = json_data['count']
        next_page = json_data['next']
        energy_data = json_data['results']
        for item in energy_data:
            # print(item['consumption'])
            c.execute("INSERT INTO rawData (consumption, startTime, endTime) VALUES(?,?,?);",
                      (item['consumption'], item['interval_start'], item['interval_end']))
        conn.commit()

        # print(next_page)

        if next_page is None:
            break
        else:
            connection_string = next_page
    conn.close()
    # end function.

def create_backup(db_name, location='./backups/'):
    date_now = datetime.now()
    current_time = date_now.strftime("%d-%m-%Y_%H_%M_%S")
    backup_file = location + 'backup'+ current_time + '.db'
    shutil.copyfile(db_name, backup_file)

def create_db(db_name):
    rawData =   "CREATE TABLE \"rawData\" (\"ID\"	INTEGER NOT NULL, " \
	            "\"consumption\"	REAL NOT NULL, " \
	            "\"endTime\"	TEXT NOT NULL, " \
	            "PRIMARY KEY(\"ID\" AUTOINCREMENT))"

    buffer = "CREATE TABLE \"buffer\" (\"ID\"	INTEGER NOT NULL, " \
             "\"consumption\"	REAL NOT NULL, " \
             "\"startTime\"	TEXT NOT NULL, " \
             "\"endTime\"	TEXT NOT NULL, " \
             "PRIMARY KEY(\"ID\" AUTOINCREMENT))"

    sData = "CREATE TABLE \"sData\" (\"rowID\"	INTEGER NOT NULL, " \
	        "\"Day\"	TEXT NOT NULL, " \
	        "\"PeakConsumption\"	REAL, " \
	        "\"OffPeakConsumption\"	REAL, " \
	        "\"TotalConsumption\"	REAL, " \
	        "PRIMARY KEY(\"rowID\"))"

    tariff =    "CREATE TABLE \"tariff\" (\"ID\"	INTEGER NOT NULL, " \
	            "\"Name\"	TEXT NOT NULL, " \
	            "\"HighTariff\"	REAL, " \
	            "\"LowTariff\"	REAL, " \
	            "\"startTime\"	TEXT, " \
	            "\"endTime\"	TEXT, " \
	            "\"StartDate\"	TEXT, " \
	            "\"endDate\"	TEXT, " \
	            "\"standingCharge\"	REAL, " \
	            "PRIMARY KEY(\"ID\" AUTOINCREMENT))"

    trigger =   "CREATE TRIGGER moveData AFTER INSERT ON buffer BEGIN " \
	            "INSERT INTO rawData (consumption, endTime) " \
	            "VALUES(NEW.consumption, NEW.endTime); END;"

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(rawData)
    c.execute(sData)
    c.execute(tariff)
    c.execute(buffer)
    c.execute(trigger)
    conn.commit()


def update_data(database, meter_point, meter_serial, api_key, start_date="2021-01-01"):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute("select Max(endTime)From rawData;")
    rows = c.fetchall()
    start = rows[0][0]
    # print(start)
    connection_string = "https://api.octopus.energy/v1/electricity-meter-points/" + meter_point + "/meters/" + meter_serial + "/consumption/"
    if start is None:
        data = {"period_from": "2021-01-01T00:00:00"}
    else:
        data = {"period_from": start}

    while True:

        res = requests.get(connection_string, verify=True, params=data, auth=HTTPBasicAuth(api_key, ''))
        data = None
        json_data = res.json()
        # print(json_data)
        number_points = json_data['count']
        next_page = json_data['next']
        energy_data = json_data['results']
        for item in energy_data:
            # print(item['consumption'])
            c.execute("INSERT INTO buffer (consumption, startTime, endTime) VALUES(?,?,?);",
                      (item['consumption'], item['interval_start'], item['interval_end']))
        conn.commit()

        # print(next_page)

        if next_page is None:
            break
        else:
            connection_string = next_page
    conn.close()


#This function is to be called after the addition of new data into the Database.
# It will call the required scripts to calculate and move the newly added data from rawData into structuredData.

def update_internal_db(database):
    # update and data transfer move queries.
    offpeak =   "INSERT INTO sData (Day, OffPeakConsumption) "\
	            "SELECT date(startTime) as valDate, "\
	            "SUM(consumption) as valTotalDay "\
	            "FROM buffer "\
	            "WHERE "\
	            "strftime('%Y', startTime)>='2021' "\
	            "AND (strftime('%H:%M:%S',startTime) >= \"00:30:00\" "\
	            "AND strftime('%H:%M:%S',startTime) < \"04:30:00\") "\
	            "GROUP BY valDate ; "\

    peak =  "UPDATE sData "\
	        "SET PeakConsumption = daily.amt "\
	        "FROM ( SELECT SUM(consumption) as amt, "\
			"date(startTime) as valDate "\
			"FROM buffer "\
			"WHERE strftime('%Y', startTime)>='2021' "\
			"AND (strftime('%H:%M:%S',startTime) >= \"00:00:00\" "\
			"AND strftime('%H:%M:%S',endTime) <= \"00:30:00\" OR strftime('%H:%M:%S',startTime) >= \"04:30:00\"" \
            " AND strftime('%H:%M:%S',endTime) <=\"23:30:00\") "\
			"GROUP BY valDate) AS daily "\
	        "WHERE sData.Day = daily.valDate; "\

    total =     "UPDATE sData "\
	            "SET TotalConsumption = daily.amt "\
	            "FROM ( SELECT date(startTime) as valDay, "\
				"	SUM(consumption) as amt "\
				"FROM buffer "\
				"WHERE "\
				"	strftime('%Y', startTime)>='2021' "\
				"	GROUP BY valDay) AS daily "\
	            "WHERE sData.Day = daily.valDay; "\


    remove_data = "DELETE FROM buffer;"

    conn = sqlite3.connect(database)  # 'consumption.db'
    c = conn.cursor()
    # print("updating offpeak")
    c.execute(offpeak);
    conn.commit()
    # print("updating peak")
    c.execute(peak)
    conn.commit()
    # print("updating total")
    c.execute(total)
    # print("delete all data")
    c.execute(remove_data)
    conn.commit()
    pass

def usage():
    print('Octopus Energy script')
    print('usage: octo.py <option> <argument>')
    print('    option:')
    print('        init database.db - initialise database with the name database.db')
    print('        update database.db - update energy data into database.db')
    print('        backup database.db location - create a copy of the DB and store it in the specified location')
    print('')
    print('Your connection details and energy meter data must be stored in a file named meter_data.py')
    # exit()  # end the program here.


# Main function
if __name__ == '__main__':

# setup logging.
    now = datetime.now()
    run_time_stamp = now.strftime('%Y-%m-%d_%H_%M')
    logging.basicConfig(filename=run_time_stamp+'.log', level=logging.DEBUG)

# parse arguments
    args = len(sys.argv)
    # print(args)
    # print(sys.argv)
# if not enough arguments, show how this is used.
    if args == 1:
        logging.error('not enough arguments')
        usage()
# if init, but not enough arguments passed, show how this is used.
    if sys.argv[1] == 'init' and args == 3:
        create_db(sys.argv[2])
        logging.info('system initialised')
        exit()
    else:
        logging.error('init: not enough arguments')
        usage()
# if update but not enough arguments passed, show how this is used.
    if sys.argv[1] == 'update' and args == 3:
        update_data(database=sys.argv[2], meter_point=meterPoint, meter_serial=meterSerial, api_key=API_Key)
        update_internal_db(sys.argv[2])
        logging.info('system updated')
        exit()
    else:
        logging.error('update: not enough arguments')
        usage()

# if backup but not enough arguments passed, show how this is used.
    if sys.argv[1] == 'backup' and args >= 3:
        if args == 3:
            create_backup(sys.argv[2])
        else:
            create_backup(sys.argv[2], location = sys.argv[3])
            logging.info('system backed up')
        exit()
    else:
        logging.error('backup: not enough arguments')
        usage()




    # create a new DB if you need to.
    # create_db('Data5.db')

    # pull all your data from your Octopus feed.
    # update_data(database='Data5.db', meter_point=meterPoint, meter_serial=meterSerial, api_key=API_Key)

    # update internal data. This will move data from the raw format into a daily totalised value per day.
    # update_internal_db('Data5.db')

    # create a backup of the database.
    #create_backup('Data4.db', )


