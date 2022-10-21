from pyspark.sql import SparkSession

ss = SparkSession.builder.master("local[2]").appName("Word count").getOrCreate()
sc = ss.sparkContext


load_data = sc.textFile("../data/word_count_sample_data.txt")
converted_data = load_data.flatMap(lambda line: line.split(" "))
assigned_data = converted_data.map(lambda x: (x, 1))
word_count_data = assigned_data.reduceByKey(lambda x, y: x+y)

final_count = 0
for (word, count) in word_count_data.collect():
    final_count = final_count + count

print(final_count)