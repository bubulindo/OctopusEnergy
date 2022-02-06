# This is the first attempt.

# This is a project to pull and analyse data from Octopus Energy


# Your API key is: sk_live_WCpBZyy7RieVyGdrZsquE7fj
API_Key = "sk_live_WCpBZyy7RieVyGdrZsquE7fj"
Account_Number = "A-9ABA5D61"
meterPoint = "1023478317662"
meterSerial = "17P2027411"
#Get consumption
# res = requests.get("https://api.octopus.energy/v1/electricity-meter-points/1023478317662/meters/17P2027411/consumption/", verify=True, auth=HTTPBasicAuth(API_Key, ''))
#
#
from dateutil import parser
from datetime import datetime
import json
import requests
import sqlite3
from requests.auth import HTTPBasicAuth
import shutil


# This needs can be tweaked to increase the period from and to.
# This could be tweaked to have the user name the table to hold the data.
def pull_data_from_octopus(database, meter_point, meter_serial, api_key):
    # add sanitation for file name
    conn = sqlite3.connect(database)
    c = conn.cursor()
    # check if database exists or create a new one.
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

def create_backup():
    date_now = datetime.now()
    current_time = date_now.strftime("%d-%m-%Y_%H_%M_%S")
    backup_file = './backups/Backup_' + current_time + '.db'
    shutil.copyfile('consumption.db', backup_file)


def update_data(database, meter_point, meter_serial, api_key):
    conn = sqlite3.connect(database) #'consumption.db'
    c = conn.cursor()
    c.execute("select MAX(endTime) from rawData")
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

        # print(next_page)

        if next_page is None:
            break
        else:
            connection_string = next_page
    conn.close()


#Initialise DB
# This function creates a DB in the correct format to be used by the Octopus logger.
def initialise_db(dbname):
    pass

#This function is to be called after the addition of new data into the Database.
# It will call the required scripts to calculate and move the newly added data from rawData into structuredData.

def update_internal_db(database):
    # update and data transfer move queries.
    offpeak =   "INSERT INTO structuredData (Year, Month, Day, OffPeakConsumption) " \
                "SELECT strftime('%Y', startTime) as valYear, " \
	            "strftime('%m', startTime) as valMonth, " \
	            "strftime('%d', startTime) as valDay, " \
	            "SUM(consumption) as valTotalDay " \
	            "FROM rawData " \
	            "WHERE " \
	            "strftime('%Y', startTime)>='2021' " \
	            "AND (strftime('%H:%M:%S',startTime) >= \"00:30:00\" " \
	            "AND strftime('%H:%M:%S',startTime) < \"04:30:00\") " \
	            "GROUP BY valYear, valMonth, valDay ;"
    peak = "UPDATE structuredData "\
	        "SET PeakConsumption = daily.amt "\
	        "FROM ( SELECT SUM(consumption) as amt, "\
			"strftime('%d', startTime) as valDay, "\
			"strftime('%m', startTime) as valMonth, "\
			"strftime('%Y', startTime) as valYear "\
			"FROM rawData "\
			"WHERE strftime('%Y', startTime)>='2021' "\
			"AND (strftime('%H:%M:%S',startTime) >= \"00:00:00\" "\
			"AND strftime('%H:%M:%S',endTime) <= \"00:30:00\" OR " \
            "strftime('%H:%M:%S',startTime) >= \"04:30:00\" AND strftime('%H:%M:%S',endTime) <=\"23:30:00\") "\
			"GROUP BY valYear, valMonth, valDay) AS daily "\
	        "WHERE structuredData.Day = CAST(daily.valDay as INTEGER) "\
	        "and structuredData.Month = CAST(daily.valMonth as INTEGER) "\
	        "and structuredData.Year = CAST(daily.valYear as INTEGER);"
    total =     "UPDATE structuredData "\
	            "SET TotalConsumption = daily.amt "\
	            "FROM (  SELECT strftime('%d', startTime) as valDay, "\
					"strftime('%m', startTime) as valMonth, "\
					"strftime('%Y', startTime) as valYear, "\
					"SUM(consumption) as amt "\
				"FROM rawData "\
				"WHERE "\
					"strftime('%Y', startTime)>='2021' "\
					"GROUP BY valYear, valMonth, valDay) AS daily "\
	"WHERE structuredData.Day = CAST(daily.valDay as INTEGER) "\
	"and structuredData.Month = CAST(daily.valMonth as INTEGER) "\
	"and structuredData.Year = CAST(daily.valYear as INTEGER);"

    remove_data = "DELETE * FROM rawData;"

    conn = sqlite3.connect(database)  # 'consumption.db'
    c = conn.cursor()
    print("updating offpeak")
    c.execute(offpeak);
    conn.commit()
    print("updating peak")
    c.execute(peak)
    conn.commit()
    print("updating total")
    c.execute(total)
    conn.commit()
    pass



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    #result = requests.get("https://api.octopus.energy/v1/electricity-meter-points/1023478317662/meters/17P2027411/consumption/", params = data, verify=True, auth=HTTPBasicAuth(API_Key, ''))
    #print(result.json())

    update_data(database='DevelopmentV2.db', meter_point=meterPoint, meter_serial=meterSerial, api_key=API_Key)
    update_internal_db('DevelopmentV2.db')
    # create_backup()

    #conn = sqlite3.connect('consumption.db')
    #c = conn.cursor()
    #c.execute("select MAX(endTime) from rawData")
    #rows = c.fetchall()
    #print(rows[0][0])

    # t_date = parser.parse(time)
    # print(t_date.day)
    # print(t_date.month)

