--update off peak consumption

INSERT INTO sData (Day, OffPeakConsumption)
	SELECT date(startTime) as valDate,
	SUM(consumption) as valTotalDay
	FROM rawData
	WHERE
	strftime('%Y', startTime)>='2021'
	AND (strftime('%H:%M:%S',startTime) >= "00:30:00"
	AND strftime('%H:%M:%S',startTime) < "04:30:00")
	GROUP BY valDate ;


-- Update the Peak Consumption

UPDATE sData
	SET PeakConsumption = daily.amt
	FROM ( SELECT SUM(consumption) as amt,
			date(startTime) as valDate
			FROM rawData
			WHERE strftime('%Y', startTime)>='2021'
			AND (strftime('%H:%M:%S',startTime) >= "00:00:00"
			AND strftime('%H:%M:%S',endTime) <= "00:30:00" OR strftime('%H:%M:%S',startTime) >= "04:30:00" AND strftime('%H:%M:%S',endTime) <="23:30:00")
			GROUP BY valDate) AS daily
	WHERE sData.Day = daily.valDate;

-- add total consumption
UPDATE sData
	SET TotalConsumption = daily.amt
	FROM (  SELECT date(startTime) as valDay,
					SUM(consumption) as amt
				FROM rawData
				WHERE
					strftime('%Y', startTime)>='2021'
					GROUP BY valDay) AS daily
	WHERE sData.Day = daily.valDay;

-- get total consumption per month
select strftime("%Y-%m", Day) as month,
        sum (PeakConsumption) as peak,
        sum(OffPeakConsumption) as offpeak ,
        sum(TotalConsumption) as Tot
from sData
group by month

-- get total per week?

