# -*- coding: UTF-8 -*- 
#Test app for spark
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import avg
import time
import org.apache.spark.rdd.PairRDDFunctions

import re

sc = SparkContext("local", "Test App")
cols = {}

#sqlContext = SQLContext(sc)

spark = SparkSession(sc)

def run():

	#df = sqlContext.read.load('airbnb_datasets/listings_us.csv',
	#					 format='com.databricks.spark.csv',
	#					 header='true',
	#					 delimiter="\t",
	#					 inferSchema='true')


	#df.select(avg('price')).show()

	data = load_data_array("airbnb_datasets/listings_us.csv")
	header = data.first()
	data = data.filter(lambda line: line != header)

	#To see the different headers
	#print(header)

	set_cols(header)

	#start_time = time.time()
	#df.select(avg('price')).show()

	#prints number of columns. Answer: 95
	#print(count_col(cols))
	#print(get_count(data))
	#print(count_col(data))
	#print(distinct_values(data))
	#print(distinct_cities(data))
	#print(get_city_array(data))
	#print(first_elem(data))
	#print(get_max_beds(data))
	#print(get_min_beds(data))
	#print(get_beds_array(data))
	#print(get_max_price(data))
	#print(get_min_price(data))
	#print(get_price_array(data))
	#print(a_price_pernight(data))
	#print_price(data)
	#print_min_nights(data)
	#print_max_nights(data)
	#print_country(data)
	#print(number_of_each_country(data))

	#print(a_price_pernight_city(data))
	#print(a_price_pernight_room_city(data))
	#print("---%s sec ---" % (time.time() - start_time))

	print(avg_list_host(data))

def load_data_array(path):
	raw_data = sc.textFile(path).cache()
	sample = raw_data.sample(False, 1, 12345)

	return sample.map(lambda line: line.split('\t'))

#Count data. Answer: 64650
def get_count(data):
	return data.count()

#Saves the index to every column
def set_cols(header):
	i = 0
	for el in header:
		cols[str(el)] = i
		i+=1

#Count numbers of columns: Answer: 95
def count_col(cols):
	#df = createDataFrame(data, schema=None, samplingRatio=None)
	return len(cols)

#TASK 1.3.2

#B) Answer: [31126, 16, 53500, 31, 366, 61, 91, 17, 5, 12, 17, 38, 20, 66, 7, 3, 216, 3, 3, 63294, 1, 117, 2266, 17, 2, 32204, 91, 3, 51117, 3, 3, 96, 1495, 13797, 51213, 98, 5, 2708, 51213, 96, 51117, 266, 35655, 64649, 2, 30393, 2, 7, 1177, 21, 6, 64649, 880, 64649, 64649, 43, 262, 56093, 68, 1282, 63031, 34231, 22976, 293, 64624, 669, 27, 2, 2, 2, 2, 2, 2, 10, 10, 10, 10, 10, 62, 10, 880, 3, 7, 262, 374, 43821, 194, 14, 12102, 59284, 56093, 36628, 1137, 56093, 415]
def distinct_values(data):
	distinct_count = []
	for i in range(0, len(cols)):
		count = data.map(lambda x: x[i]).distinct().count()
		distinct_count.append(count)
	return distinct_count

#C) Number of distincts cities: Answer: 3
def distinct_cities(data):
	return data.map(lambda line: line[cols['city']]).distinct().count()

#C) Print citynames and number: Answer: NY, Seattle, SF, None ... 3
def get_city_array(data):
	out = data.map(lambda x: x[cols['city']]).distinct()
	cities = out.collect()
	for city in cities:
		print(city.encode('utf-8').strip())
	return out.count()

#D) Chooses:
#Finds the highest number of beds. Answer: 16
def get_max_beds(data):
	return data.map(lambda x: to_float(x[cols['beds']])).max()
#Returns the lowest number of beds. Answer: 0
def get_min_beds(data):
	return data.map(lambda x: to_float(x[cols['beds']])).min()

#Distinct values for beds
def get_beds_array(data):
	out = data.map(lambda x: x[cols['beds']]).distinct()
	beds = out.collect()
	for bed in beds:
		print(bed.encode('utf-8').strip())
	return out.count()
#Hva med aa finne hvor mange av hver verdi?

#Price

def get_max_price(data):
	return data.map(lambda x: to_float(x[cols['price']])).max()

def get_min_price(data):
	return data.map(lambda x: to_float(x[cols['price']])).min()

def get_price_array(data):
	out = data.map(lambda x: x[cols['price']]).distinct()
	price = out.collect()
	for p in price:
		print(p.encode('utf-8').strip())
	return out.count()

def a_price_pernight(data):
	return data.map(lambda x: to_float(x[cols['price']])).mean()

def print_price(data):
	out = data.map(lambda x: to_float(x[cols['price']]))
	mini = out.min()
	maxi = out.max()
	avg = out.mean()
	print("Minimum price is $%.2f. \nMaximum price is $%.2f. \nAverage price is $%.2f" % (mini, maxi, avg)) 
	return 

#Minimum nights
def print_min_nights(data):
	out = data.map(lambda x: to_float(x[cols['minimum_nights']]))
	mini = out.min()
	maxi = out.max()
	avg = out.mean()
	print("Min minimum nights is %.2f. \nMax minimum nights is %.2f. \nAverage minimum nights is %.2f" % (mini, maxi, avg)) 
	return 

#Maximum nights
def print_max_nights(data):
	out = data.map(lambda x: to_float(x[cols['maximum_nights']]))
	mini = out.min()
	maxi = out.max()
	avg = out.mean()
	print("Min maximum nights is %.2f. \nMax maximum nights is %.2f. \nAverage maximum nights is %.2f" % (mini, maxi, avg)) 
	return 

#Country
def print_country(data):
	out = data.map(lambda x: x[cols['country']]).distinct()
	countries = out.collect()
	for country in countries:
		print(country.encode('utf-8').strip())
	return out.count()
	
def number_of_each_country(data):
	out = data.map(lambda x: x[cols['country']])
	num = out.reduceByKey(lambda accum, n: accum + n)
	return num.collect() #DETTE HAR JEG SPORSMAAL OM

#Float
def to_float(string):
	try:
		return float(string.replace(',', '').replace('$', ''))
	except ValueError, e:
		return 0


	