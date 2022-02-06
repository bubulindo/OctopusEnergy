# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# This is a project to pull and analyse data from Octopus Energy






from dateutil import parser
from datetime import datetime
import json
import requests
import sqlite3
from requests.auth import HTTPBasicAuth
import shutil
from meter_data import _API_key as API_key
from meter_data import _Account_Number as Account_Number
from meter_data import _meterPoint as meterPoint
from meter_data import _meterSerial as meterSerial

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

def create_backup(db_name):
    date_now = datetime.now()
    current_time = date_now.strftime("%d-%m-%Y_%H_%M_%S")
    backup_file = './backups/Backup_' + current_time + '.db'
    shutil.copyfile(db_name, backup_file)

def create_db(db_name):
    rawData =   "CREATE TABLE \"rawData\" (\"ID\"	INTEGER NOT NULL, " \
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

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(rawData)
    c.execute(sData)
    c.execute(tariff)
    conn.commit()


def update_data(database, meter_point, meter_serial, api_key, start_date="2021-01-01"):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute("select Max(Day)From sData;")
    rows = c.fetchall()
    start = rows[0][0]
    print(start)
    connection_string = "https://api.octopus.energy/v1/electricity-meter-points/" + meter_point + "/meters/" + meter_serial + "/consumption/"
    while True:
        data = {"period_from": start}
        res = requests.get(connection_string, verify=True, params=data, auth=HTTPBasicAuth(api_key, ''))

        json_data = res.json()
        number_points = json_data['count']
        next_page = json_data['next']
        energy_data = json_data['results']
        for item in energy_data:
            # print(item['consumption'])
            c.execute("INSERT INTO rawData (consumption, startTime, endTime) VALUES(?,?,?);",
                      (item['consumption'], item['interval_start'], item['interval_end']))
        conn.commit()

        print(next_page)

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
	            "FROM rawData "\
	            "WHERE "\
	            "strftime('%Y', startTime)>='2021' "\
	            "AND (strftime('%H:%M:%S',startTime) >= \"00:30:00\" "\
	            "AND strftime('%H:%M:%S',startTime) < \"04:30:00\") "\
	            "GROUP BY valDate ; "\

    peak =  "UPDATE sData "\
	        "SET PeakConsumption = daily.amt "\
	        "FROM ( SELECT SUM(consumption) as amt, "\
			"date(startTime) as valDate "\
			"FROM rawData "\
			"WHERE strftime('%Y', startTime)>='2021' "\
			"AND (strftime('%H:%M:%S',startTime) >= \"00:00:00\" "\
			"AND strftime('%H:%M:%S',endTime) <= \"00:30:00\" OR strftime('%H:%M:%S',startTime) >= \"04:30:00\"" \
            " AND strftime('%H:%M:%S',endTime) <=\"23:30:00\") "\
			"GROUP BY valDate) AS daily "\
	        "WHERE sData.Day = daily.valDate; "\

    total =     "UPDATE sData "\
	            "SET TotalConsumption = daily.amt "\
	            "FROM (  SELECT date(startTime) as valDay, "\
				"	SUM(consumption) as amt "\
				"FROM rawData "\
				"WHERE "\
				"	strftime('%Y', startTime)>='2021' "\
				"	GROUP BY valDay) AS daily "\
	            "WHERE sData.Day = daily.valDay; "\

    remove_data = "DELETE FROM rawData;"

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



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # create a new DB if you need to.
    #create_db('Data3.db')

    # pull all your data from your Octopus feed.
    update_data(database='Data3.db', meter_point=meterPoint, meter_serial=meterSerial, api_key=API_Key)

    # update internal data. This will move data from the raw format into a daily totalised value per day.
    update_internal_db('Data3.db')

    # create a backup of the database.
    create_backup('Data3.db')


