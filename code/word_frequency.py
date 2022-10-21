from pyspark.sql import SparkSession

ss = SparkSession.builder.master("local[2]").appName("Word Frequency").getOrCreate()
sc = ss.sparkContext

load_data = sc.textFile("../data/word_frequency_sample_data.txt")

converted_data = load_data.flatMap(lambda line: line.split(" "))

lower_case_data = converted_data.map(lambda word: word.lower())

assigned_data = lower_case_data.map(lambda x: (x, 1))

word_frequency_data = assigned_data.reduceByKey(lambda x, y: x+y)

print(word_frequency_data.collect())