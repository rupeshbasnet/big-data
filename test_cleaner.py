import pyspark
from pyspark.sql import SQLContext
import csv
import pandas as pd
import dateutil

# Clean Taxi Data
def parserTaxi(id, data):
    if id == 0:
        data.next()
    for i in csv.reader(data):
        tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, \
        pickup_latitude, dropoff_longitude, dropoff_latitude, Date, Weekday = i[1], i[2], i[5], i[6], i[9], i[10], \
        datetime.datetime.strptime(i[1], "%Y-%m-%d %H:%M:%S").date(), \
        calendar.day_name[datetime.datetime.strptime("2016-06-09 21:06:36", "%Y-%m-%d %H:%M:%S").weekday()]
        
        # rows required for the taxi data
        yield tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, \
        pickup_latitude, dropoff_longitude, dropoff_latitude, Date, Weekday

if __name__ == '__main__':
	sc = pyspark.SparkContext()
	taxi = sc.textFile('hdfs:///user/rbasnet000/Data/yellow_tripdata_2016-01.csv', use_unicode=False).cache() 
	taxiRDD = taxi.mapPartitionsWithIndex(parserTaxi)
	df = sqlContext.createDataFrame(taxiRDD, ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'Date', 'Weekday'])
	df.coalesce(1).write.csv('hdfs:///user/rbasnet000/CleanData/yellow_tripdata_2016-01_clean', header=True)
