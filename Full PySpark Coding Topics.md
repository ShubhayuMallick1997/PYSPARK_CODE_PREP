# **Full PySpark Coding Topics**

---

### ✅ 1. **Setting Up PySpark**

**Goal**: Install and initialize PySpark.

#### 🔹 Installation

```bash
pip install pyspark
```

#### 🔹 Starting a SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()
```

---

### ✅ 2. **Understanding SparkContext & SparkSession**

#### 🔹 SparkConf & SparkContext

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("App").setMaster("local[*]")
sc = SparkContext(conf=conf)
```

#### 🔹 SparkSession (modern entry point)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataApp") \
    .getOrCreate()
```

> Use `spark` for DataFrames, SQL, Hive, ML, etc. `sc` is used for RDDs.

---

### ✅ 3. **Creating RDDs and Basic Operations**

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

#### 🔹 Transformations

```python
mapped = rdd.map(lambda x: x * 2)
filtered = rdd.filter(lambda x: x > 3)
```

#### 🔹 Actions

```python
print(mapped.collect())
print(filtered.count())
```

---

### ✅ 4. **Creating DataFrames**

#### 🔹 From RDD

```python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()
```

#### 🔹 From CSV/JSON/Parquet

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

---

### ✅ 5. **DataFrame Transformations**

```python
df = df.withColumn("age_plus_5", df["age"] + 5)
df = df.filter(df["age"] > 25)
df = df.drop("age_plus_5")
df = df.withColumnRenamed("name", "full_name")
```

---

### ✅ 6. **Aggregations & Grouping**

```python
df.groupBy("department").agg({"salary": "avg"}).show()

from pyspark.sql.functions import avg
df.groupBy("department").agg(avg("salary")).show()
```

#### 🔹 Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("row_num", row_number().over(windowSpec)).show()
```

---

### ✅ 7. **Joins in PySpark**

```python
df1.join(df2, df1.id == df2.emp_id, "inner").show()
```

Types of joins:

* inner
* left
* right
* outer
* semi
* anti

> Use `broadcast(df)` when joining large with small dataset.

---

### ✅ 8. **File Formats (CSV, JSON, Parquet)**

#### 🔹 Reading Files

```python
spark.read.csv("data.csv", header=True).show()
spark.read.json("data.json").show()
spark.read.parquet("data.parquet").show()
```

#### 🔹 Writing Files

```python
df.write.mode("overwrite").parquet("output/")
```

---

### ✅ 9. **Using SQL in PySpark**

```python
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 30").show()
```

---

### ✅ 10. **UDFs (User Defined Functions)**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def greet(name):
    return f"Hello, {name}"

greet_udf = udf(greet, StringType())
df = df.withColumn("greeting", greet_udf(df["name"]))
```

---


---

### ✅ **1. Setting Up PySpark**

#### 🧩 Why It Matters:

PySpark is the Python API for Apache Spark. It's used for distributed processing of large datasets.

#### 🧪 Installation

```bash
pip install pyspark
```

> This installs all Spark dependencies (including JVM bindings).

#### 🛠 Initialize SparkSession (for DataFrame API)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()
```

* `appName`: Sets the name seen on Spark UI
* `master`: `local[*]` runs on all CPU cores; for cluster you’d use `yarn`, `mesos`, or `spark://...`

---

### ✅ **2. SparkContext & SparkSession**

#### 🔹 SparkContext

Used for working with **RDDs** (low-level API).

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("App").setMaster("local")
sc = SparkContext(conf=conf)
```

#### 🔹 SparkSession (modern API)

Most data engineering work today uses SparkSession.

```python
spark = SparkSession.builder.getOrCreate()
```

> **Best practice:** Prefer `SparkSession` for new work; it internally creates `SparkContext`.

---

### ✅ **3. RDDs (Resilient Distributed Datasets)**

RDDs are fault-tolerant collections of objects, spread across a cluster. Mostly used for **low-level transformations**.

#### 📌 Create RDD

```python
data = [1, 2, 3, 4]
rdd = sc.parallelize(data)
```

#### 🔁 Transformations

```python
rdd.map(lambda x: x * 2).collect()       # [2, 4, 6, 8]
rdd.filter(lambda x: x > 2).collect()    # [3, 4]
```

#### ✅ Actions

```python
rdd.reduce(lambda a, b: a + b)           # 10
rdd.count()                              # 4
```

#### ⚠️ Lazy Evaluation

Operations don’t execute until an **action** is called (e.g., `collect()`, `count()`).

---

### ✅ **4. Creating DataFrames**

Most PySpark data engineering is done using **DataFrames**.

#### 📌 From Python object:

```python
data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()
```

#### 📁 From CSV/JSON/Parquet:

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df = spark.read.json("data.json")
df = spark.read.parquet("data.parquet")
```

> **Real-world tip**: Prefer **Parquet** for analytics (compressed, columnar, schema-evolved)

---

### ✅ **5. DataFrame Transformations**

These are transformations over columns in a DataFrame.

#### 🎯 Column Ops

```python
from pyspark.sql.functions import col

df.withColumn("age_plus_5", col("age") + 5)
df = df.drop("temp_col")
df = df.withColumnRenamed("dob", "date_of_birth")
```

#### 🔍 Filters

```python
df.filter(col("age") > 25).show()
```

#### 🧠 Complex logic

```python
from pyspark.sql.functions import when

df = df.withColumn("category", when(col("age") > 30, "Senior").otherwise("Junior"))
```

---

### ✅ **6. Aggregations & Grouping**

#### 🔸 Basic Aggregations

```python
df.groupBy("department").agg({"salary": "avg"}).show()
```

#### 🔸 With functions

```python
from pyspark.sql.functions import avg, sum

df.groupBy("dept").agg(avg("salary").alias("avg_sal")).show()
```

#### 🔸 Window Functions

Used to compute values **over a partition** of the data.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("dept").orderBy("salary")
df = df.withColumn("row_num", row_number().over(window_spec))
```

---

### ✅ **7. Joins in PySpark**

#### 📎 Syntax

```python
df1.join(df2, df1.id == df2.emp_id, "inner")
```

#### 🔸 Types:

* `"inner"`: default
* `"left"`, `"right"`, `"outer"`
* `"left_semi"`: like `EXISTS` subquery
* `"left_anti"`: like `NOT EXISTS`

#### 💡 Performance Tip:

Use `broadcast()` when joining small dataset with a large one:

```python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "id")
```

---

### ✅ **8. Working with Files**

#### 📂 Read

```python
df = spark.read.csv("file.csv", header=True)
df = spark.read.json("file.json")
df = spark.read.parquet("file.parquet")
```

#### 💾 Write

```python
df.write.mode("overwrite").parquet("output/")
```

#### 🔸 Compression

```python
df.write.option("compression", "snappy").parquet("output/")
```

---

### ✅ **9. PySpark SQL**

Allows you to use SQL queries on DataFrames.

```python
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 30").show()
```

> Very useful for **ad hoc exploration**, **debugging**, and **cross-functional work**.

---

### ✅ **10. UDFs (User Defined Functions)**

Use when PySpark functions aren’t enough (not preferred unless necessary).

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def greet(name):
    return "Hello " + name

greet_udf = udf(greet, StringType())

df = df.withColumn("greeting", greet_udf(df["name"]))
```

> ❗ UDFs break optimizations and can be slower. Use built-in functions when possible.

---

Great! Let’s now continue with the **next set of PySpark coding topics**, covering:

* 🏭 **Partitioning & Performance Optimization**
* 🧠 **MLlib (Machine Learning)**
* 📦 **Broadcast Variables & Accumulators**
* 🔄 **Streaming & Structured Streaming**

---

### ✅ **11. Partitioning & Performance Optimization**

Efficient data partitioning and execution tuning are critical in large-scale Spark jobs.

#### 🔹 Repartition vs Coalesce

```python
df = df.repartition(10)    # Increase partitions (shuffles)
df = df.coalesce(2)        # Reduce partitions (no shuffle)
```

* Use `repartition()` for even distribution (expensive).
* Use `coalesce()` when reducing partitions before writing.

#### 🔹 Caching / Persisting

```python
df.cache()        # Keeps data in memory (RAM)
df.persist()      # Allows memory + disk + more options
df.unpersist()
```

* Cache repeated DataFrame usage (e.g., joins, multiple actions)

#### 🔹 Catalyst Optimizer

Automatically rewrites and optimizes logical plans. Avoids redundant computations.

#### 🔹 Tungsten Execution Engine

Optimizes physical execution (memory + CPU use). Handles:

* Bytecode generation
* Off-heap memory
* Whole-stage codegen

#### 🔹 Handle Data Skew (Salting)

If one value (like country='US') appears a lot, join gets skewed.

```python
# Add salt key to skewed column
from pyspark.sql.functions import concat, lit, rand

df1 = df1.withColumn("salted_key", concat(df1["id"], (rand() * 10).cast("int")))
```

> 🔥 Real-World Tip: Partition properly when writing to S3/Parquet — avoid small files and skew.

---

### ✅ **12. PySpark MLlib (Machine Learning)**

MLlib is Spark’s built-in scalable machine learning library.

#### 🔸 Pipeline Components

```python
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

indexer = StringIndexer(inputCol="gender", outputCol="gender_index")
vec = VectorAssembler(inputCols=["age", "income", "gender_index"], outputCol="features")
lr = LogisticRegression(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[indexer, vec, lr])
model = pipeline.fit(train_df)
```

#### 🔸 Prediction

```python
predictions = model.transform(test_df)
predictions.select("label", "prediction").show()
```

#### 🔸 CrossValidator

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.1]).build()
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, numFolds=3)
cv_model = cv.fit(train_df)
```

> ✨ MLlib works well on large clusters. For complex tasks (NLP, CV), integrate with **TensorFlowOnSpark**, **Horovod**, or move to **pandas API** for small-scale.

---

### ✅ **13. Broadcast Variables & Accumulators**

Used to **share read-only data** across workers, and for **global counters**.

#### 🔸 Broadcast Variable

```python
bc_dict = sc.broadcast({"IN": "India", "US": "United States"})

def get_country_name(code):
    return bc_dict.value.get(code, "Unknown")

rdd.map(lambda x: get_country_name(x)).collect()
```

* Avoids sending same dictionary to each executor.

#### 🔸 Accumulator

```python
error_count = sc.accumulator(0)

def count_error(row):
    global error_count
    if row.status == "error":
        error_count += 1

rdd.foreach(count_error)
print(error_count.value)
```

---

### ✅ **14. PySpark Streaming & Structured Streaming**

Real-time processing of data from Kafka, files, sockets, etc.

#### 🔸 Structured Streaming

```python
stream_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .load("input_dir/")

query = stream_df.writeStream \
    .format("console") \
    .start()
query.awaitTermination()
```

#### 🔸 Kafka Source Example

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .load()
```

#### 🔸 Windowed Aggregations

```python
from pyspark.sql.functions import window

df.groupBy(window("timestamp", "10 minutes")).count()
```

> 🔥 Real-Time Use Case: Ingest log data from Kafka → aggregate → write to sink (S3, dashboard, DB)

---

Perfect! Let's move ahead with the next advanced PySpark topics:

---

### 🔐 **15. Working with AWS and Cloud**

Modern data pipelines often involve storing, processing, and retrieving data from the cloud—most commonly AWS.

#### 🔸 Reading and Writing to S3

```python
df = spark.read.csv("s3a://my-bucket/input.csv")

df.write.parquet("s3a://my-bucket/output/", mode="overwrite")
```

* Use `s3a://` protocol.
* Set Hadoop AWS dependencies & credentials (`spark.hadoop.fs.s3a.access.key` etc.)

#### 🔸 Connect to Snowflake

Use Snowflake Spark Connector:

```python
df = spark.read \
    .format("snowflake") \
    .option("sfURL", "<account>.snowflakecomputing.com") \
    .option("sfUser", "user") \
    .option("sfPassword", "pass") \
    .option("sfDatabase", "DB") \
    .option("sfWarehouse", "WAREHOUSE") \
    .option("sfSchema", "SCHEMA") \
    .option("dbtable", "TABLE") \
    .load()
```

#### 🔸 AWS Secrets Manager (with Boto3)

```python
import boto3
secret_client = boto3.client("secretsmanager")
secret = secret_client.get_secret_value(SecretId="my-spark-secret")
```

#### 🔸 Running Jobs on AWS EMR

You can submit jobs using:

* `spark-submit` on EMR master node
* AWS Step Functions
* Apache Airflow with `EmrAddStepsOperator`

#### 🔸 Glue vs EMR

| Feature          | Glue                      | EMR                    |
| ---------------- | ------------------------- | ---------------------- |
| Serverless       | ✅                         | ❌ (You manage cluster) |
| Ideal for        | ETL + small jobs          | Complex, large-scale   |
| Cost             | Pay per run               | Pay for uptime         |
| Language Support | PySpark + Scala (limited) | Full Spark + libraries |

---

### 🪄 **16. Airflow Integration with PySpark**

Airflow is commonly used to orchestrate PySpark pipelines.

#### 🔸 Define DAG to Run Spark Job

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG("pyspark_etl", start_date=datetime(2024, 1, 1), schedule_interval="@daily")

spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command="spark-submit /path/to/job.py",
    dag=dag
)
```

#### 🔸 Using EMR Operators

```python
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
```

* You can add Spark steps dynamically to EMR clusters.

#### 🔸 Trigger via PythonOperator

```python
def run_spark():
    os.system("spark-submit your_script.py")

PythonOperator(
    task_id="submit_spark",
    python_callable=run_spark,
    dag=dag
)
```

---

### 🧪 **17. Testing and Debugging PySpark**

Unit testing and debugging PySpark code ensures reliability and maintainability.

#### 🔸 Using `unittest` or `pytest`

```python
import unittest
from pyspark.sql import SparkSession

class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()

    def test_row_count(self):
        df = self.spark.createDataFrame([(1, "Alice")], ["id", "name"])
        self.assertEqual(df.count(), 1)
```

#### 🔸 Debug Locally vs Cluster

* Use `.explain()` to understand execution plan.
* Use Spark UI for jobs/stages analysis.

#### 🔸 Logging

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Spark job started")
```

---

### 🧰 **18. DevOps & Deployment for PySpark**

PySpark projects in production require version control, CI/CD, and automation.

#### 🔸 Git Best Practices

* Modularize your scripts.
* Store `requirements.txt`, `.env`, and `Dockerfile`.

#### 🔸 Packaging

Use `setuptools` or `pip`:

```bash
pip install -e .
```

#### 🔸 CI/CD with Jenkins or GitHub Actions

* Test PySpark scripts
* Deploy to EMR/Glue
* Trigger via cron or commit

#### 🔸 Scheduling Options

* Apache Airflow
* cron + spark-submit
* AWS Step Functions

---

Great! Let’s now dive into the final section of our PySpark learning roadmap:

---

### 📚 **19. Advanced Topics in PySpark**

These are high-performance, enterprise-grade capabilities used in **modern data lakehouses** and **big data platforms**. Mastering these will set you apart as a senior-level data engineer.

---

#### 🧪 **Delta Lake with PySpark**

Delta Lake brings **ACID transactions** to Apache Spark.

```python
from delta.tables import DeltaTable

# Writing in Delta format
df.write.format("delta").save("/tmp/delta-table")

# Reading Delta
delta_df = spark.read.format("delta").load("/tmp/delta-table")

# Update via DeltaTable API
dt = DeltaTable.forPath(spark, "/tmp/delta-table")
dt.update(condition="id = 1", set={"value": "'updated'"})
```

🔹 **Key Features**:

* ACID transactions
* Time Travel (`.option("versionAsOf", 2)`)
* Schema Evolution
* Data versioning and auditability

---

#### 🧊 **Apache Iceberg**

Iceberg is a **table format** for managing huge analytic datasets on the lakehouse.

```python
spark.read.format("iceberg").load("my_catalog.db.table_name")
```

* Supports hidden partitioning (no need to manage manually)
* Integrates with Trino, Presto, Spark, Flink
* Useful for multi-engine query workloads

---

#### 🏛 **Lakehouse Architecture**

It combines **data lake flexibility** with **data warehouse features**.

| Feature            | Data Lake | Data Warehouse | Lakehouse |
| ------------------ | --------- | -------------- | --------- |
| Schema Enforcement | ❌         | ✅              | ✅         |
| ACID Transactions  | ❌         | ✅              | ✅         |
| Performance        | Medium    | High           | High      |
| Cost               | Low       | High           | Medium    |

💡 Tools: Delta Lake, Apache Hudi, Apache Iceberg enable this.

---

#### 🔢 **Z-Ordering in Delta Lake**

Z-Ordering helps optimize **read performance** on high-cardinality columns.

```python
df.write.format("delta").option("dataChange", "false").saveAsTable("my_table")
spark.sql("OPTIMIZE my_table ZORDER BY (user_id)")
```

* Great for selective queries (e.g., `WHERE user_id = 123`).
* Requires Databricks or open-source Delta Lake with `OPTIMIZE`.

---

#### ⚙️ **Performance Tuning for Large Jobs**

🔧 Key practices:

* **Broadcast joins** for small lookup tables.

* Use `repartition()` or `coalesce()` strategically.

* Filter early → `select` columns early (projection pushdown).

* Tune Spark config:

  ```python
  .config("spark.sql.shuffle.partitions", "100")
  .config("spark.executor.memory", "4g")
  ```

* Monitor with **Spark UI** (Jobs, Stages, Executors tabs).

---




