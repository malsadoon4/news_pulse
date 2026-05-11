"""
bonus_clustering.py — Bonus: TF-IDF + KMeans topic clustering

Reads all ingested headlines in batch mode and clusters them into 3 topics.
Run after data/incoming/ has several JSON batches:

  python bonus_clustering.py
"""

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

spark = (
    SparkSession.builder
    .appName("NewsPulse-Clustering")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("ts", TimestampType(), True),
    ]
)

df = spark.read.schema(schema).json("data/incoming")

if df.rdd.isEmpty():
    print("No headlines found. Run ingester.py first.")
    raise SystemExit(0)

print(f"Loaded {df.count()} headlines for clustering.")

tokenizer = Tokenizer(inputCol="title", outputCol="words_raw")
remover = StopWordsRemover(inputCol="words_raw", outputCol="words")
hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=1000)
idf = IDF(inputCol="raw_features", outputCol="features")
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster")

pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, kmeans])
model = pipeline.fit(df)
clustered = model.transform(df)

print("\n=== Topic Clusters ===")
for cluster_id in range(3):
    print(f"\n--- Cluster {cluster_id} ---")
    (
        clustered
        .filter(F.col("cluster") == cluster_id)
        .select("source", "title")
        .show(5, truncate=80)
    )
