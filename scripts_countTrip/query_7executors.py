if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import sys
    from pyspark.sql.functions import desc
    spark = SparkSession.builder \
        .appName("Best Driver 2018") \
        .config("spark.sql.shuffle.partitions", "7") \
	    .config("spark.executor.memory", "2g") \
	    .config("spark.executor.instances", "7") \
	    .config("spark.executor.cores", "1") \
	    .getOrCreate()

    schema = "VendorID INT,pickup_datetime TIMESTAMP,dropoff_datetime TIMESTAMP,store_and_fwd_flag STRING,RatecodeID INT,PULocationID INT,DOLocationID INT,passenger_count INT,trip_distance FLOAT,fare_amount FLOAT,extra FLOAT,mta_tax FLOAT,tip_amount FLOAT,tolls_amount FLOAT,ehail_fee FLOAT,improvement_surcharge FLOAT,total_amount FLOAT,payment_type INT,trip_type INT,congestion_surcharge FLOAT"
    
    driver_df = spark.read.csv(path=sys.argv[1], schema=schema, header=True).select("VendorID")
    
    count_trip = driver_df.groupBy("VendorID").count().sort(desc("count"))
    count_trip.explain()
    count_trip.show()
