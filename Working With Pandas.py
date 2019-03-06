# Databricks notebook source
# MAGIC %md
# MAGIC # Working With pandas Dataframes in Databricks
# MAGIC <img src='https://pandas.pydata.org/_static/pandas_logo.png' />
# MAGIC 
# MAGIC In Databricks, we typically work with Spark dataframes.  They are great for doing distributed computing on a cluster.
# MAGIC However, there are many Python libraries that are not designed for distributed computing.  They use dataframes from the [pandas](https://pandas.pydata.org/)
# MAGIC library.  These datafames are designed to be stored in the memory of a single machine, not distributed across a cluster.
# MAGIC 
# MAGIC *Is there a way we can distribute pandas-based Python code across a cluster?*
# MAGIC 
# MAGIC Yes, you can!  Spark 2.3 added a feature called "[Pandas User-Defined Functions](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)"
# MAGIC (or "UDF" for short).  This provides the ability to define low-overhead, high-performance UDFs entirely in Python.
# MAGIC 
# MAGIC Here's how it works:
# MAGIC  - Write a Python function that accepts a pandas dataframe as the input
# MAGIC  - Divide your Spark dataframe into groups using the `groupBy` method
# MAGIC  - Each group will be converted to a pandas dataframe and passed to your function. These will be executed in parallel as each group's processing can occur on a different executor.
# MAGIC  
# MAGIC Let's walk through an example!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Some Data Into Spark
# MAGIC First, we'll make a Spark dataframe.

# COMMAND ----------

# Read in sample data from a CSV file and infer the schema
df = spark\
      .read\
      .option("inferSchema", "true")\
      .option("header", "true")\
      .csv("/databricks-datasets/flights/departuredelays.csv")

display(df)

# COMMAND ----------

# In this dataset, flights originated from 255 unique airports
df.select("origin").distinct().count()

# COMMAND ----------

# How many flights originated from each airport?
display(df.groupBy("origin").count().sort("origin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Python Function
# MAGIC 
# MAGIC Now we want to create a user-defined function that performs our calculations.  There are two types of pandas UDF's in Spark:  Scalar and Grouped-Map.
# MAGIC Scalar functions accept a single pandas *series* (i.e. a single column).  Grouped-map functions accept a whole pandas dataframe, and they can operate on all of the
# MAGIC data in that dataframe.  We'll be using a grouped-map function for our example.
# MAGIC 
# MAGIC Grouped-map pandas UDFs are designed for the "split-apply-combine" pattern of data analysis, and they operate on all the data for some group.
# MAGIC (e.g., "for each airport, apply this operation")  Grouped-map pandas UDFs first split a Spark DataFrame into groups based on the conditions specified
# MAGIC in the `groupBy` operator, applies a user-defined function (pandas.DataFrame -> pandas.DataFrame) to each group, and then combines all of the results
# MAGIC into a new Spark DataFrame.
# MAGIC 
# MAGIC ### Declaring the Output Schema
# MAGIC When you declare your Python function as a pandas UDF, you must provide the schema that will be used for the new dataframe that you are creating.  In our case,
# MAGIC we're going to use the structure of the existing dataframe and just add some new columns.  So we can start with the schema of the Spark dataframe and append the
# MAGIC definintions of our new columns.

# COMMAND ----------

from pyspark.sql.types import *

# Start with the fields already in the Spark dataframe
output_schema_fields = [f for f in df.schema.fields]

# Add the new fields that we will be creating in our UDF
output_schema_fields.append(StructField("run_id", StringType(), True))
output_schema_fields.append(StructField("hostname", StringType(), True))
output_schema_fields.append(StructField("airport_average_delay", DoubleType(), True))

# Convert the list of fields to a Spark "StructType" schema
output_schema = StructType(output_schema_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring the Function
# MAGIC When you declare your Python function as a pandas UDF, you must use the `pandas_udf` decorator.  This decorator accepts two parameters: the schema of the output
# MAGIC Spark dataframe (which we just created) and the type of pandas UDF (in our case, "GROUPED_MAP").
# MAGIC 
# MAGIC Inside the function, you work with the pandas dataframe just like you would in straight Python. You can add or remove columns or rows.  You can even
# MAGIC synthesize the data into a new pandas dataframe with a totally different structure and return that back to Spark.
# MAGIC 
# MAGIC In our example below, we will do three things:
# MAGIC   1. Compute a new unique string and apply the value as a new column to all rows in the dataset.  This will prove that each grouping is processed separately.
# MAGIC   2. Add a new column with the hostname of the computer that is doing the processing.  This will prove that the computation has been distributed to different machines in the cluster.
# MAGIC   3. Compute the average delay time for the airport as a demonstration of how you can compute using a pandas dataframe
# MAGIC   
# MAGIC Let's write our function!

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
import uuid
import socket

@pandas_udf(output_schema, PandasUDFType.GROUPED_MAP)
# Input/output are both a pandas.DataFrame
def my_pandas_function(pdf):
  # 1. Compute a guid and assign it as a column to all rows in the pandas dataframe
  my_run_id = str(uuid.uuid4())
  pdf["run_id"] = my_run_id
  
  # 2. Add a new column that contains the name of the computer on which this code is being executed
  pdf["hostname"] = socket.gethostname()
  
  # 3. Compute the average delay for the airport in this grouping and add that as a column to the output pandas dataframe
  pdf["airport_average_delay"] = pdf["delay"].mean()

  return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our pandas UDF, we just perform a `groupBy` on our Spark dataframe and then `apply` the UDF to the groupings.  This will run the UDF on each grouping,
# MAGIC collect all the results, and then combine them into a *new* Spark dataframe.

# COMMAND ----------

new_df = df.groupBy("origin").apply(my_pandas_function)
display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's run some checks against `new_df` to make sure Spark is processing the data as we expected.  All of the rows for an airport should have been processed by
# MAGIC a single call to the `my_pandas_function` UDF.  Therefore, we would expect all of the rows for each airport to have the same `run_id` value and to have the same
# MAGIC `hostname` value.  We'll use a distinct count to make sure that each airport has only one value for those columns.

# COMMAND ----------

from pyspark.sql.functions import *

new_df\
  .groupBy("origin")\
  .agg(
    count("*").alias("row_count"),
    countDistinct("run_id").alias("run_id_count"),
    countDistinct("hostname").alias("hostname_count")
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Next, since each group of airports was processed separately, we would expect the number of unique `run_id` values to be the same as the number of unique airport codes.
# MAGIC Let's check!

# COMMAND ----------

print(new_df.select("origin").distinct().count())
print(new_df.select("run_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC We also expect the code to be executed across different machines in the cluster.  If we group by `hostname`, we should see one row for each node in our cluster.  Also, the number of rows processed by each node should be roughly symetrical.

# COMMAND ----------

display(new_df.groupBy("hostname").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Lastly, the `airport_average_delay` value should be the same for every row for a given airport.  We can also use Spark to check our math and make sure that the
# MAGIC average was calculated correctly.

# COMMAND ----------

new_df\
  .groupBy("origin")\
  .agg(
    min("airport_average_delay").alias("min_avg_delay"),
    max("airport_average_delay").alias("max_avg_delay"),
    avg("delay").alias("avg_computed_by_spark")
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC So there you have it.  Spark allows you to distribute execution of your pure Python functions across a cluster by dividing the data into groups, converting each group
# MAGIC to a pandas dataframe, and then processing each group in parallel.
