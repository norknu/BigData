#Test app for spark
from pyspark import SparkContext
from pyspark.sql.functions import avg

import re

sc = SparkContext("local", "Test App")
cols = {}

def run():
	data = load_data_array("airbnb_datasets/listings_us.csv")
	header = data.first()
	data = data.filter(lambda line: line != header)

	# To see the different headers
	#print(header)

	# Gets the cols and related col index
	set_cols(header)

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
	print(a_price_pernight(data))


#Prints the first element of the file as a list = column names
def first_elem(data):
	return data.first()

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

#Count numbers of columns
def count_col(cols):
	#df = createDataFrame(data, schema=None, samplingRatio=None)
	return len(cols)

#Task 1.3.2
#b) Answer: [31127, 17, 53501, 32, 367, 62, 92, 18, 6, 13, 18]
def distinct_values(data):
	distinct_count = []

	for i in range(0, len(cols)):
		count = data.map(lambda x: x[i]).distinct().count()
		distinct_count.append(count)

	return distinct_count

#c) Number of distincts cities: Answer: 274
def distinct_cities(data):
	return data.map(lambda x: x[cols['city']].lower()).distinct().count()

#c) Print citynames
def get_city_array(data):
	cities = data.map(lambda x: x[cols['city']].lower()).distinct().collect()
	for city in cities:
		print(city.encode('utf-8').strip())

#d) Chooses:

#Beds, find min, max ...
#Finds the highest number of beds. Answer: 16
def get_max_beds(data):
	return data.max(lambda x: to_float(x[cols['beds']]))[cols['beds']]
#Returns the lowest number of beds. Answer: 0
def get_min_beds(data):
	min_val = data.min(lambda x: to_float(x[cols['beds']]))[cols['beds']]
	return to_float(min_val)

# Need this function to handle empty strings
def to_float(string):
	try:
		return float(re.sub("[^0-9^.]", "", string))
	except ValueError, e:
		return 0

#Task 1.3.3

#a
#Avarage bookingprice per night
def a_price_pernight(data):
	return data.map(lambda x: to_float(x[cols['price']])).mean()


run()
	