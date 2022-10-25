from pyspark.sql import SparkSession

ss = SparkSession.builder.master("local[2]").appName("revenue by customer").getOrCreate()
sc = ss.sparkContext

customer_data = sc.textFile("../data/customer_orders_sample_data.csv")
customer_price_data = customer_data.map(lambda x: (x.split(",")[0], x.split(",")[2]))
revenue_by_customer = customer_price_data.reduceByKey(lambda x, y: float(x) + float(y))

sorted_revenue = revenue_by_customer.sortBy(lambda x: x[1], False)

for r in sorted_revenue.collect():
    print(r)