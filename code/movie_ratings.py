from pyspark.sql import SparkSession

ss = SparkSession.builder.master("local[2]").appName("movie ratings").getOrCreate()
sc = ss.sparkContext

movie_data = sc.textFile("../data/movie_ratings_sample_data.csv")
rating_movie_id_data = movie_data.map(lambda x: (x.split("\t")[2], 1))
movie_ratings = rating_movie_id_data.reduceByKey(lambda x,y: x+y)

for i in movie_ratings.collect():
    print(i)


avg_by_movie_id = movie_data.map(lambda x: (x.split("\t")[0], x.split("\t")[2])).reduceByKey(lambda x,y: round((int(x)+int(y))/2))
rating_count = avg_by_movie_id.map(lambda x: (x[1], 1))
count_by_rating = rating_count.reduceByKey(lambda x,y:x+y)

for i in count_by_rating.collect():
    print(i)