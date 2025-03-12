from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("RealtimeVoting") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                "org.scala-lang:scala-library:2.12.18") \
        .config('spark.sql.adaptive.enable', 'false') \
        .getOrCreate()

    # print(spark)

    vote_schema = StructType([
        StructField('voter_id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', TimestampType(), True),
        StructField('vote', IntegerType(), True),
    ])

    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vote_schema).alias("data")) \
        .select("data.*")

    enriched_votes_df = votes_df.withWatermark('voting_time', '1 minute')

    # Aggregation
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id").agg(_sum("vote").alias("total_votes"))

    query = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "/home/davidntd/PycharmProjects/RealTime_DataEngineeringProject_Ballond'Or_FootBall_VotingSystem/checkpoints/checkpoint1") \
        .outputMode("complete") \
        .start()

    # query = votes_per_candidate.writeStream\
    #     .format('console')\
    #     .outputMode('update')\
    #     .start()

    query.awaitTermination()