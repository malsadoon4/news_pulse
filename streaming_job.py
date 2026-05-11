import os
import json
import time
import threading

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

os.makedirs("C:/tmp/spark", exist_ok=True)
os.makedirs("data/incoming", exist_ok=True)

os.environ["HADOOP_HOME"] = "C:\\hadoop"

spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp/spark")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ts", TimestampType(), True),
])

stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 10)
    .json("data/incoming")
)

STOP_WORDS = [
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
    "is", "are", "was", "were", "be", "been", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "it", "its", "this", "that", "from", "by", "as",
    "not", "no", "can", "more", "also", "they", "he", "she", "we", "you", "their", "there",
    "how", "what", "when", "where", "who", "which", "all", "one", "his", "her", "our",
    "your", "my", "just", "now", "here", "say", "says", "said", "get", "use", "new", "two",
    "after", "before", "during", "while", "since", "into", "over", "than", "then", "so",
]

q_src = (
    stream
    .groupBy("source")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("by_source")
    .start()
)

q_win = (
    stream
    .withWatermark("ts", "2 hours")
    .groupBy(F.window("ts", "1 hour"))
    .count()
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("count")
    )
    .writeStream
    .outputMode("append")
    .format("memory")
    .queryName("by_window")
    .start()
)

q_words = (
    stream
    .select(
        F.split(
            F.regexp_replace(F.lower(F.col("title")), r"[^a-zA-Z\s]", ""),
            "\\s+"
        ).alias("tokens")
    )
    .select(F.explode("tokens").alias("word"))
    .filter(~F.col("word").isin(STOP_WORDS))
    .filter(F.length("word") >= 3)
    .groupBy("word")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("top_words")
    .start()
)

print("[streaming_job] Three streaming queries started!")
print("[streaming_job] Waiting for data in data/incoming/ ...")


def dump_results():
    while True:
        try:
            src = spark.sql("SELECT * FROM by_source").toPandas()
            win = spark.sql("SELECT * FROM by_window").toPandas()
            kw = spark.sql(
                "SELECT * FROM top_words ORDER BY count DESC LIMIT 15"
            ).toPandas()

            results = {
                "by_source": src.to_dict(orient="records"),
                "by_window": [
                    {
                        "window_start": str(r["window_start"]),
                        "count": int(r["count"])
                    }
                    for _, r in win.iterrows()
                ],
                "top_words": kw.to_dict(orient="records"),
                "updated_at": time.strftime("%H:%M:%S"),
            }

            with open("results.json", "w") as f:
                json.dump(results, f)

            print(f"[dump] updated at {results['updated_at']}")

        except Exception as e:
            print(f"[dump] skipped: {e}")

        time.sleep(10)


threading.Thread(target=dump_results, daemon=True).start()

spark.streams.awaitAnyTermination()