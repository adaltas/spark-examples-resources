if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import desc, broadcast
    spark = SparkSession.builder \
        .appName("Best Driver 2018") \
        .config("spark.sql.shuffle.partitions", "8") \
    	.config("spark.executor.memory", "1g") \
    	.config("spark.executor.instances", "4") \
    	.config("spark.executor.cores", "2") \
    	.getOrCreate()

    schema = "VendorID INT,pickup_datetime TIMESTAMP,dropoff_datetime TIMESTAMP,store_and_fwd_flag STRING,RatecodeID INT,PULocationID INT,DOLocationID INT,passenger_count INT,trip_distance FLOAT,fare_amount FLOAT,extra FLOAT,mta_tax FLOAT,tip_amount FLOAT,tolls_amount FLOAT,ehail_fee FLOAT,improvement_surcharge FLOAT,total_amount FLOAT,payment_type INT,trip_type INT,congestion_surcharge FLOAT"
    
    driver_df = spark.read.csv(path="/home/ferdinand.de-baecque-dsti/nyctrip/trip_taxi.csv", schema=schema, header=True).select("VendorID")

    littleDf = spark.createDataFrame([(1, "The First Company"), (2, "The second company")], ("VendorID","VendorName"))

    detailledDfBroad = driver_df.join(broadcast(littleDf), driver_df["VendorID"] == littleDf["VendorID"])
    detailledDfBroad.explain()

    detailledDfBroad.groupBy("VendorName").count().sort(desc("count")).show()
