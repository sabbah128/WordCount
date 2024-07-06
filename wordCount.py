from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("Kian_Spark").getOrCreate()

results = (
    spark.read.text("YOUR_FILE.txt")
    .select(F.explode(F.split(F.lower(F.col("value")), " ")).alias("word"))
    .select(F.regexp_replace(F.col("word"), "[^a-z_-]", "").alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
    .orderBy(F.col("count").desc())
    .write.csv('word_coun.csv', header=True)
)