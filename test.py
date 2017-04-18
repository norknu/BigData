#Test app for spark
from pyspark import SparkContext

sc = SparkContext("local", "Test App")

def run():
	data = load_data_array("Data/listings_us.csv")
	print(get_count(data))
	print(distinct_values(data))

def load_data_array(path):
	raw_data = sc.textFile(path).cache()
	sample = raw_data.sample(False, 1, 12345)
	return sample.map(lambda line: line.split('\t'))

def get_count(data):
	return data.count()

#Task 1.3.2
#b)

def distinct_values(data):
	distinct_count = []

	for i in range(0, 11):
		count = data.map(lambda x: x[i]).distinct().count()
		distinct_count.append(count)

	return distinct_count


run()
	