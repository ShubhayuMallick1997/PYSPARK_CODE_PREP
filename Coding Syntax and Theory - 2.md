# **Coding Syntax and Theory - 2:**

### 🧑‍💻 **Step 1: Setting Up PySpark**

#### ✅ **Goal**: Install PySpark and create a SparkSession.

#### 🔧 **Installation (Local/Standalone):**

```bash
pip install pyspark
```

#### 🔥 **Start SparkSession (Python code):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BasicExample") \
    .getOrCreate()
```

#### 📌 **Notes:**

* `SparkSession` is the entry point to PySpark DataFrames and SQL.
* Use `.getOrCreate()` to avoid creating multiple sessions.

---

### 🧠 **Step 2: SparkContext & SparkConf**

#### ✅ **Goal**: Understand and configure the SparkContext.

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)
```

#### 🔁 **Useful Configs:**

```python
conf.set("spark.executor.memory", "2g")
conf.set("spark.executor.cores", "2")
```

#### 📌 **Notes:**

* `SparkConf` sets environment-level properties.
* `SparkContext` is the main entry point for low-level RDD APIs.

---

### 📦 **Step 3: Creating RDDs**

#### ✅ **Goal**: Learn different ways to create RDDs.

```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.textFile("data/sample.txt")  # Reads each line as a string
```

#### 🧾 **Check contents:**

```python
rdd1.collect()
```

#### 📌 **Notes:**

* `parallelize()` creates RDD from local Python objects.
* `textFile()` reads files into RDD.

---

### 🔁 **Step 4: RDD Transformations**

#### ✅ **Goal**: Apply transformations like `map`, `flatMap`, `filter`.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

rdd_map = rdd.map(lambda x: x * 2)
rdd_filter = rdd.filter(lambda x: x % 2 == 0)
rdd_flatMap = sc.parallelize(["a b", "c d"]).flatMap(lambda x: x.split(" "))
```

#### 📌 **Notes:**

* Transformations are lazy; they return a new RDD.

---

### 🚀 **Step 5: RDD Actions**

#### ✅ **Goal**: Perform computations using `collect`, `count`, `reduce`.

```python
rdd = sc.parallelize([1, 2, 3, 4])
print(rdd.collect())       # Returns all elements
print(rdd.count())         # Returns count
print(rdd.reduce(lambda x, y: x + y))  # Returns sum
```

#### 📌 **Notes:**

* Actions trigger actual job execution in Spark.
* Use `take(n)` to fetch first `n` elements instead of `collect()` on big data.

---

### 💾 **Step 6: RDD Persistence**

#### ✅ **Goal**: Cache or persist RDDs for performance.

```python
rdd = sc.textFile("data.txt")
rdd.cache()
rdd.count()
```

#### 📌 **Notes:**

* `.cache()` keeps RDD in memory.
* `.persist()` allows memory + disk/storage options.

---

### 📊 **Step 7: Creating DataFrames**

#### ✅ **Goal**: Convert RDD to DataFrame or read files directly.

```python
from pyspark.sql import Row

rdd = sc.parallelize([Row(name="Alice", age=25), Row(name="Bob", age=30)])
df = spark.createDataFrame(rdd)
df.show()
```

#### Or from file:

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

#### 📌 **Notes:**

* DataFrames are preferred over RDDs for structured data.
* `inferSchema=True` automatically detects column types.

---

### 🧾 **Step 8: Defining Schema**

#### ✅ **Goal**: Provide custom schema for DataFrame.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.csv("data.csv", header=False, schema=schema)
df.show()
```

#### 📌 **Notes:**

* Helpful when you know column types beforehand.
* Prevents `inferSchema` from misinterpreting values.

---

### 🔧 **Step 9: Basic DataFrame Operations**

```python
df.select("name").show()
df.filter(df.age > 25).show()
df.orderBy("age").show()
```

#### 📌 **Notes:**

* Supports SQL-like transformations.
* Chainable and optimized under the hood.

---

### 🧪 **Step 10: Column Operations**

```python
from pyspark.sql.functions import col

df.withColumn("age_plus_10", col("age") + 10).show()
df.drop("age").show()
df.select(col("name").alias("username")).show()
```

---

Excellent! Let’s now **elaborate on each of the 10 basic PySpark coding points** with deeper explanation, **real-world usage, code examples**, and key tips. This will help you become confident in the fundamentals before jumping into intermediate-level PySpark coding.

---

### 🧑‍💻 **Step 1: Setting Up PySpark**

#### 🔸 What is it?

Before you write any PySpark code, you need to initialize a **SparkSession**, which acts as the gateway to interact with Spark’s APIs.

#### 🔸 Code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()
```

#### 🔸 Real World:

Used in every project. Often the first few lines of any PySpark job/script.

#### 🔸 Tips:

* Use `.master("yarn")` in a cluster environment.
* Always call `stop()` after job completion to free resources.

---

### 🧠 **Step 2: SparkContext & SparkConf**

#### 🔸 What is it?

* `SparkContext`: The low-level object that connects your application to the Spark cluster.
* `SparkConf`: Used to define the configuration (app name, memory, cores).

#### 🔸 Code:

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("LowLevelApp").setMaster("local[2]")
sc = SparkContext(conf=conf)
```

#### 🔸 Real World:

Used mainly in older Spark versions or when using RDDs.

#### 🔸 Tips:

* Don't mix SparkSession and manual SparkContext unless you’re sure.
* Useful in fine-tuned applications that use RDDs for performance.

---

### 📦 **Step 3: Creating RDDs**

#### 🔸 What is it?

RDD (Resilient Distributed Dataset) is the **original Spark abstraction** for handling distributed data in memory.

#### 🔸 Code:

```python
# From a local list
rdd = sc.parallelize([1, 2, 3, 4])

# From a text file
rdd_file = sc.textFile("data.txt")
```

#### 🔸 Real World:

Used when you need fine control over transformations or working with unstructured data.

#### 🔸 Tips:

* Use RDDs for complex data transformations not supported in DataFrames.
* RDDs are fault-tolerant and support in-memory computation.

---

### 🔁 **Step 4: RDD Transformations**

#### 🔸 What is it?

Transformations create a new RDD from the original. They’re **lazy** — not executed until an action is called.

#### 🔸 Code:

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

mapped = rdd.map(lambda x: x * 2)
filtered = rdd.filter(lambda x: x % 2 == 0)
flat_mapped = sc.parallelize(["a b", "c d"]).flatMap(lambda x: x.split(" "))
```

#### 🔸 Real World:

Used in ETL tasks like cleansing, mapping raw log files, parsing tokens, etc.

---

### 🚀 **Step 5: RDD Actions**

#### 🔸 What is it?

Actions **trigger execution** and return actual results.

#### 🔸 Code:

```python
rdd = sc.parallelize([1, 2, 3, 4])
rdd.collect()      # [1, 2, 3, 4]
rdd.count()        # 4
rdd.reduce(lambda a, b: a + b)  # 10
```

#### 🔸 Real World:

Used to evaluate, aggregate, or extract sample data in production workflows.

#### 🔸 Tips:

* Avoid `.collect()` on huge datasets to prevent memory errors.

---

### 💾 **Step 6: RDD Persistence**

#### 🔸 What is it?

Keeps the computed RDD in memory for reuse. Prevents recomputation, saving time.

#### 🔸 Code:

```python
rdd = sc.textFile("bigfile.txt")
rdd.cache()
rdd.count()
```

#### 🔸 Real World:

Used in iterative algorithms (e.g., ML training) or repetitive reads.

#### 🔸 Tips:

* Use `.persist(StorageLevel.MEMORY_AND_DISK)` if data may not fit in memory.

---

### 📊 **Step 7: Creating DataFrames**

#### 🔸 What is it?

DataFrames are distributed collections of data with a **named schema** (columns).

#### 🔸 Code:

```python
# From RDD
from pyspark.sql import Row
rdd = sc.parallelize([Row(name="John", age=30), Row(name="Doe", age=25)])
df = spark.createDataFrame(rdd)

# From CSV
df2 = spark.read.csv("employees.csv", header=True, inferSchema=True)
```

#### 🔸 Real World:

Used in almost every modern Spark job. It enables optimization using Catalyst engine.

---

### 🧾 **Step 8: Defining Schema**

#### 🔸 What is it?

Allows you to explicitly define column names and types.

#### 🔸 Code:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.csv("data.csv", schema=schema, header=False)
```

#### 🔸 Real World:

Used when inferSchema guesses wrong or data types are critical for downstream processing.

---

### 🔧 **Step 9: Basic DataFrame Operations**

#### 🔸 Code:

```python
df.select("name").show()
df.filter(df["age"] > 30).show()
df.orderBy(df["age"].desc()).show()
```

#### 🔸 Real World:

Used in data filtering, reporting dashboards, and interactive notebooks (e.g., Databricks).

---

### 🧪 **Step 10: Column Operations**

#### 🔸 Code:

```python
from pyspark.sql.functions import col, when

df.withColumn("new_age", col("age") + 5).show()
df.drop("age").show()
df.select(col("name").alias("employee_name")).show()
df.withColumn("status", when(col("age") > 30, "Senior").otherwise("Junior")).show()
```

#### 🔸 Real World:

Used in transformations, feature engineering, creating derived columns.

---

Great! Let’s now go **step-by-step through the intermediate-level PySpark coding topics**, elaborating each with:

* ✅ What it means
* ⚙️ When and why you use it
* 🧠 Code examples
* 💼 Real-world scenarios
* 🪛 Best practices

---

### 🧮 **Step 11: Aggregations in PySpark**

#### ✅ What it means:

Aggregations are used to summarize or group data using functions like `count()`, `sum()`, `avg()`, `min()`, `max()`, etc.

#### ⚙️ When to use:

Whenever you need to derive **metrics** like total sales, average age, group counts, etc.

#### 🧠 Code example:

```python
from pyspark.sql.functions import avg, count, sum, min, max

df.groupBy("department").agg(
    count("*").alias("num_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
).show()
```

#### 💼 Real-world:

In HR analytics to find average salaries by department, or in sales to find revenue by region.

#### 🪛 Tip:

Use `.cache()` before aggregating large DataFrames to avoid recomputation.

---

### 📊 **Step 12: Window Functions**

#### ✅ What it means:

Window functions perform **operations over a window/partition of data**, enabling row-wise computations like rankings, lags, and running totals.

#### ⚙️ When to use:

Needed when you want results **per row but within a group**, like top-N records, rank-based rewards, or time-based calculations.

#### 🧠 Code example:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

window_spec = Window.partitionBy("department").orderBy("salary")

df.withColumn("rank", rank().over(window_spec)).show()
```

#### 💼 Real-world:

Used in leaderboard systems, salary rankings, customer recency scoring.

#### 🪛 Tip:

Choose `dense_rank()` if you want to avoid skipped numbers after ties.

---

### 🔁 **Step 13: Joins in PySpark**

#### ✅ What it means:

Joining lets you combine rows from multiple DataFrames based on a condition.

#### ⚙️ When to use:

To merge customer details with transaction data, or link employee tables with manager tables.

#### 🧠 Code example:

```python
df1.join(df2, df1["emp_id"] == df2["id"], "inner").show()
```

#### Types:

* Inner
* Left, Right, Full Outer
* Semi, Anti
* Broadcast (for optimization)

#### 💼 Real-world:

Used across data lakes to stitch tables from different domains (e.g., user data + app logs).

#### 🪛 Tip:

Use broadcast joins (`broadcast(df2)`) if one DataFrame is small to reduce shuffle.

---

### 📂 **Step 14: Working with Files (CSV, JSON, Parquet, etc.)**

#### ✅ What it means:

PySpark can read/write data in multiple formats.

#### ⚙️ When to use:

For ingesting or exporting data from/to storage like S3, HDFS, or local.

#### 🧠 Code example:

```python
# Reading
df = spark.read.option("header", True).csv("employees.csv")

# Writing
df.write.mode("overwrite").parquet("output_data/")
```

#### 💼 Real-world:

Used in pipelines to read source data and save processed outputs.

#### 🪛 Tip:

Use `.option("compression", "snappy")` with Parquet for space efficiency.

---

### 🧾 **Step 15: PySpark SQL**

#### ✅ What it means:

You can use SQL syntax on top of DataFrames by registering them as temporary views.

#### ⚙️ When to use:

If you're comfortable with SQL or want to allow analysts to run ad-hoc queries.

#### 🧠 Code example:

```python
df.createOrReplaceTempView("employees")
spark.sql("SELECT department, AVG(salary) FROM employees GROUP BY department").show()
```

#### 💼 Real-world:

Heavily used in Data Warehousing and BI tools (e.g., Tableau + Spark SQL).

#### 🪛 Tip:

Use SQL for readability when handling complex logic like nested queries.

---

### ⚙️ **Step 16: UDFs (User Defined Functions)**

#### ✅ What it means:

UDFs allow you to use **custom Python logic** in your DataFrame operations.

#### ⚙️ When to use:

When built-in functions can’t express your transformation.

#### 🧠 Code example:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize(salary):
    return "High" if salary > 50000 else "Low"

udf_func = udf(categorize, StringType())
df.withColumn("salary_category", udf_func(df["salary"])).show()
```

#### 💼 Real-world:

Used in feature engineering, tagging, sentiment labeling, etc.

#### 🪛 Tip:

Prefer `pandas_udf` for better performance if working with large data.

---

### 🏭 **Step 17: Repartition vs Coalesce**

#### ✅ What it means:

Both are used to **control the number of partitions** in a DataFrame.

* `repartition(n)` – Shuffles data for equal partitioning
* `coalesce(n)` – Reduces partitions without shuffle

#### ⚙️ When to use:

* Use `repartition` before wide transformations (e.g., join)
* Use `coalesce` before writing output

#### 🧠 Code:

```python
df = df.repartition(10)  # For parallelism
df.write.mode("overwrite").coalesce(1).csv("output/")
```

#### 💼 Real-world:

Used to control file size or improve shuffle performance.

#### 🪛 Tip:

Avoid small file problem by using `coalesce()` before writing to S3.

---

### 🔐 **Step 18: Caching and Persistence**

#### ✅ What it means:

These keep data in memory (or disk) for reuse across actions, reducing recomputation.

#### ⚙️ When to use:

When you’re running multiple actions on the same transformed data.

#### 🧠 Code:

```python
df.cache()
df.count()
df.show()
```

#### 💼 Real-world:

Used in iterative jobs like ML training or multi-stage transformations.

#### 🪛 Tip:

Use `.persist(StorageLevel.MEMORY_AND_DISK)` if `.cache()` leads to OOM.

---

### 🔧 **Step 19: Tuning Spark Performance**

#### ✅ What it means:

Adjusting settings to make Spark jobs faster and cheaper.

#### Key areas:

* Partition sizing
* Using broadcast joins
* Avoiding UDFs where possible
* Using columnar formats (Parquet)
* Using `.explain()` and Spark UI for debugging

#### 💼 Real-world:

Critical in production. Poorly tuned jobs can take hours instead of minutes.

Absolutely! Let's now go step-by-step through the **intermediate PySpark coding topics** — explaining **each point in detail**, with **clear examples**, **real-world use cases**, and **best practices**.

---

### 🧮 **Step 11: Aggregations in PySpark**

#### ✅ What it is:

Aggregation refers to **grouping data** and applying aggregate functions like `sum`, `avg`, `count`, `min`, `max`, etc., to extract summaries.

#### 🧠 Example:

```python
from pyspark.sql.functions import avg, count

df = spark.createDataFrame([
    ("HR", 50000),
    ("HR", 60000),
    ("IT", 80000),
    ("IT", 90000)
], ["department", "salary"])

df.groupBy("department").agg(
    count("*").alias("total_employees"),
    avg("salary").alias("average_salary")
).show()
```

#### 📌 Output:

```
+----------+---------------+--------------+
|department|total_employees|average_salary|
+----------+---------------+--------------+
|        HR|              2|       55000.0|
|        IT|              2|       85000.0|
+----------+---------------+--------------+
```

#### 💼 Use case:

Used to find total sales per region, average salary per department, or count of logins per user.

#### 🪛 Best Practices:

* Always name the result columns with `.alias()` for clarity.
* Cache the DataFrame before heavy groupBy.

---

### 📊 **Step 12: Window Functions**

#### ✅ What it is:

Window functions let you **compute values over a group of rows** (a window), while still keeping individual rows. You can compute running totals, ranks, differences, etc.

#### 🧠 Example:

```python
from pyspark.sql.functions import rank
from pyspark.sql.window import Window

df = spark.createDataFrame([
    ("HR", "Alice", 50000),
    ("HR", "Bob", 60000),
    ("IT", "Charlie", 80000),
    ("IT", "David", 90000)
], ["department", "employee", "salary"])

window_spec = Window.partitionBy("department").orderBy(df.salary.desc())

df.withColumn("rank", rank().over(window_spec)).show()
```

#### 📌 Output:

```
+----------+--------+------+----+
|department|employee|salary|rank|
+----------+--------+------+----+
|        HR|    Bob | 60000|   1|
|        HR|  Alice | 50000|   2|
|        IT|  David | 90000|   1|
|        IT|Charlie | 80000|   2|
+----------+--------+------+----+
```

#### 💼 Use case:

Used in top-N queries, customer behavior scoring, monthly user rankings.

#### 🪛 Tip:

Use `dense_rank()` to avoid gaps in rankings.

---

### 🔁 **Step 13: Joins in PySpark**

#### ✅ What it is:

Joining combines rows from two DataFrames based on a matching column.

#### Types of Joins:

* Inner
* Left, Right, Full Outer
* Semi, Anti
* Broadcast (optimized small-table join)

#### 🧠 Example:

```python
emp = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
dept = spark.createDataFrame([(1, "HR"), (2, "IT")], ["id", "dept"])

emp.join(dept, emp.id == dept.id, "inner").show()
```

#### 📌 Output:

```
+---+-----+---+----+
| id| name| id|dept|
+---+-----+---+----+
|  1|Alice|  1|  HR|
|  2|  Bob|  2|  IT|
+---+-----+---+----+
```

#### 💼 Use case:

Joining transaction logs with customer master data.

#### 🪛 Tip:

Use `broadcast()` for one small DataFrame to avoid shuffle:

```python
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

---

### 📂 **Step 14: Working with Files**

#### ✅ What it is:

PySpark supports reading/writing various formats: CSV, JSON, Parquet, ORC, Avro.

#### 🧠 Example:

```python
# Read CSV
df = spark.read.option("header", True).csv("data/employees.csv")

# Write to Parquet
df.write.mode("overwrite").parquet("output/employees/")
```

#### 💼 Use case:

Reading daily ingestion data from S3, writing transformed output to Parquet.

#### 🪛 Tip:

* Use `.option("compression", "snappy")` for better storage.
* Partition large output files with `.partitionBy("column")`.

---

### 🧾 **Step 15: PySpark SQL**

#### ✅ What it is:

Allows running SQL queries over DataFrames by creating temporary views.

#### 🧠 Example:

```python
df.createOrReplaceTempView("employees")

spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
""").show()
```

#### 💼 Use case:

Useful for analysts or when migrating from traditional SQL engines.

#### 🪛 Tip:

Keep your SQL clean; use `EXPLAIN` to analyze query plan performance.

---

### ⚙️ **Step 16: UDFs (User Defined Functions)**

#### ✅ What it is:

Custom Python functions registered to work on DataFrames where built-in functions fall short.

#### 🧠 Example:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def tag_salary(sal):
    return "High" if int(sal) > 50000 else "Low"

tag_udf = udf(tag_salary, StringType())
df.withColumn("salary_tag", tag_udf(df["salary"])).show()
```

#### 💼 Use case:

Classify text, apply regex, complex conditions.

#### 🪛 Tip:

* UDFs are slow. Use built-in functions or `pandas_udf` where possible.

---

### 🏭 **Step 17: Repartition vs Coalesce**

#### ✅ What it is:

* `repartition(n)` – increases or resets number of partitions (with shuffle)
* `coalesce(n)` – reduces number of partitions (without shuffle)

#### 🧠 Example:

```python
df.repartition(10)       # For better parallelism
df.coalesce(1).write.csv("final_output/")
```

#### 💼 Use case:

Control file sizes before writing to HDFS or S3.

#### 🪛 Tip:

Use `coalesce(1)` to generate a single CSV output (good for small test runs).

---

### 🔐 **Step 18: Caching and Persistence**

#### ✅ What it is:

Stores DataFrames in memory or disk to prevent recomputation.

#### 🧠 Example:

```python
df.cache()
df.count()  # Triggers caching
df.show()
```

#### 💼 Use case:

Reusing transformed data across multiple operations.

#### 🪛 Tip:

Use `.persist()` with custom levels if caching fails due to memory constraints.

---

### 🔧 **Step 19: Spark Performance Tuning**

#### ✅ What it is:

Involves optimizing configurations, code, and data layout to improve job execution time and resource efficiency.

#### Key Areas:

* Use columnar formats (Parquet)
* Avoid UDFs where possible
* Optimize `spark.sql.shuffle.partitions`
* Monitor via Spark UI and `.explain()`

#### 🧠 Example:

```python
df.explain(True)
```

#### 💼 Use case:

Used heavily in production pipelines processing large volumes.

#### 🪛 Tip:

Use Spark UI DAG view + stage time + memory metrics to debug bottlenecks.

---
Tune `spark.sql.shuffle.partitions` and memory settings based on job size.

---


