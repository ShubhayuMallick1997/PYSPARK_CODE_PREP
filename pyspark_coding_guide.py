# PYSPARK CODING TOPICS WITH SYNTAX AND EXAMPLES

# ==============================
# 🔹 1. Setting up PySpark
# ==============================

# a. Creating SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .master("local[*]") \
    .getOrCreate()

# b. Getting SparkContext
sc = spark.sparkContext

# ==============================
# 🔹 2. RDD Operations
# ==============================

# a. Creating RDDs
rdd = sc.parallelize([1, 2, 3, 4, 5])

# b. Transformations
mapped_rdd = rdd.map(lambda x: x * 2)
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)

# c. Actions
print("Count:", rdd.count())
print("Collect:", rdd.collect())

# ==============================
# 🔹 3. DataFrames
# ==============================

# a. Creating DataFrames from list
from pyspark.sql import Row
data = [Row(name="John", age=30), Row(name="Jane", age=25)]
df = spark.createDataFrame(data)
df.show()

# b. Reading CSV/JSON/Parquet
csv_df = spark.read.option("header", "true").csv("file.csv")
json_df = spark.read.json("file.json")
parquet_df = spark.read.parquet("file.parquet")

# c. Writing to files
csv_df.write.mode("overwrite").csv("output/path")

# ==============================
# 🔹 4. DataFrame Transformations
# ==============================

df = df.withColumn("age_plus_10", df.age + 10)
df = df.drop("age")
df = df.withColumnRenamed("name", "full_name")
df.select("full_name").show()

# ==============================
# 🔹 5. Filtering and Conditions
# ==============================

from pyspark.sql.functions import col, when
filtered_df = df.filter(col("age") > 20)
df = df.withColumn("age_group", when(col("age") > 30, "Senior").otherwise("Junior"))

# ==============================
# 🔹 6. Grouping and Aggregations
# ==============================

df.groupBy("age").count().show()
df.groupBy("age").agg({"age": "avg"}).show()

# ==============================
# 🔹 7. Joins
# ==============================

df1 = spark.createDataFrame([(1, "John"), (2, "Jane")], ["id", "name"])
df2 = spark.createDataFrame([(1, "NY"), (2, "CA")], ["id", "state"])
joined_df = df1.join(df2, on="id", how="inner")
joined_df.show()

# ==============================
# 🔹 8. Window Functions
# ==============================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("state").orderBy("id")
df = df.withColumn("row_num", row_number().over(window_spec))

# ==============================
# 🔹 9. UDFs (User Defined Functions)
# ==============================

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def to_upper(name):
    return name.upper()

upper_udf = udf(to_upper, StringType())
df = df.withColumn("upper_name", upper_udf(col("full_name")))

# ==============================
# 🔹 10. Caching and Repartitioning
# ==============================

cached_df = df.cache()
repartitioned_df = df.repartition(4)

# ==============================
# 🔹 11. Reading/Writing from AWS S3
# ==============================

# Read
s3_df = spark.read.parquet("s3a://your-bucket/path")

# Write
s3_df.write.mode("overwrite").parquet("s3a://your-bucket/output")

# ==============================
# 🔹 12. Running SQL Queries
# ==============================

df.createOrReplaceTempView("people")
sql_df = spark.sql("SELECT * FROM people WHERE age > 25")
sql_df.show()

# ==============================
# 🔹 13. Error Handling and Debugging
# ==============================

try:
    df = spark.read.csv("nonexistent.csv")
except Exception as e:
    print("Error:", e)

# ==============================
# 🔹 14. Logging and explain()
# ==============================

df.explain(True)
spark.sparkContext.setLogLevel("INFO")

# ==============================
# 🔹 15. Job Scheduling in Airflow (Conceptual)
# ==============================

# You would define DAG and call your PySpark scripts via BashOperator or PythonOperator.

# ==============================
# 🔹 16. Unit Testing (Conceptual)
# ==============================

# Use `pytest` or `unittest` along with `assert` statements to validate transformations.

# ==============================
# 🔹 17. Machine Learning with MLlib
# ==============================

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Assuming df has columns: features and label
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df = assembler.transform(df)
model = LinearRegression(featuresCol="features", labelCol="label").fit(df)

# ==============================
# 🔹 18. Streaming (Structured Streaming)
# ==============================

stream_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
query = stream_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
