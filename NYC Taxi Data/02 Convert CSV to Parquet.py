# Databricks notebook source
# MAGIC %md
# MAGIC # Convert CSV's with NYC Taxi Data to Parquet
# MAGIC 
# MAGIC This notebook uses Spark to read the NYC Taxi Data CSV files and convert all of the data into a Parquet file.  (*Technically*, it's creating a Databricks Delta file, but that is just
# MAGIC regular ol' Parquet with some additional sidecar data.)
# MAGIC 
# MAGIC We want to combine all of the CSV's into one, master dataset.  However, the schemas for the CSV vary between taxi types and change over time within the taxi type.  Therefore, we
# MAGIC must devote some effort to standardizing the schemas to one, common schema.

# COMMAND ----------

# MAGIC %md
# MAGIC First, specify the location of the CSV files (the input for this process) and the location where we will store the Parquet output.

# COMMAND ----------

csv_path = "/mnt/taxi/csv"
parquet_path = "/mnt/taxi/parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we create a Delta table with the schema we ultimately want for our dataset.  This schema was designed by analyzing the available columns in all of the CSV files and determining all of the possible data points we need to capture.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS taxi")

sql_statement = '''
  CREATE TABLE taxi (
    taxi_type string NOT NULL,
    year int NOT NULL,
    filename string NOT NULL,
    dispatching_base_num string,
    vendor_id string,
    vendor_name string,
    pickup_datetime timestamp,
    pickup_location_id string,
    pickup_latitude decimal(9, 5),
    pickup_longitude decimal(9, 5),
    dropoff_datetime timestamp,
    dropoff_location_id string,
    dropoff_latitude decimal(9, 5),
    dropoff_longitude decimal(9, 5),
    passenger_count integer,
    trip_distance decimal(9, 5),
    trip_type string,
    payment_type string,
    rate_code string,
    shared_ride string,
    store_and_forward string,
    fare_amount decimal(8, 2),
    extra decimal(8, 2),
    mta_tax decimal(8, 2),
    tip_amount decimal(8, 2),
    tolls_amount decimal(8, 2),
    surcharge decimal(8, 2),
    ehail_fee decimal(8, 2),
    total_amount decimal(8, 2)
  )
  USING DELTA
  PARTITIONED BY (taxi_type, year)
  LOCATION '{}'
'''.format(parquet_path)

spark.sql(sql_statement)

# COMMAND ----------

# MAGIC %md
# MAGIC Here's a little function that will recursively search subdirectories in the DBFS and return all of the files in the directory tree.

# COMMAND ----------

def getFiles(path):
  files = dbutils.fs.ls(path)
  output = []
  
  for f in files:
    if f.name.endswith("/"):
      output.extend(getFiles(f.path))
    else:
      output.append(f)
  
  return(output)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we do the real work!
# MAGIC 
# MAGIC 1. Recursively get all of the CSV files from the input location
# MAGIC 2. Loop through all of the files and read each file in as a Spark dataframe.  Don't apply any schema; just read whatever columns we find as strings
# MAGIC 3. We add new columns to each dataframe containing the taxi type and the name of the CSV file from which the data was read
# MAGIC 4. Some of the CSV files have leading spaces in the column names.  We rename those columns to remove those spaces and to convert all column names to lowercase (for consistency)
# MAGIC 5. The names of the columns can vary between files.  So we've hard-coded a mapping to convert column names to a standard name.
# MAGIC 6. For any columns with a name ending in "datetime", parse the string contents of the column into a timestamp
# MAGIC 7. Add a new column by extracting the year from the `pickup_datetime` column
# MAGIC 8. Compare the schema of our dataframe to the target Delta table.  If there are any columns missing from our dataframe, add them with `null` values.
# MAGIC 9. Loop through all of the columns in our dataframe.  If a column's datatype doesn't match the datatype in the target Delta table, cast the column to the appropriate datatype.
# MAGIC 10. Reorder the columns in our dataframe to match the order of columns in the target Delta table.
# MAGIC 11. At last!  We insert the dataframe into the Delta table.  This writes the data out as Parquet in our target location.
# MAGIC 12. Go back to Step 2 and process the next CSV file.

# COMMAND ----------

import pyspark.sql.functions as pyf
from pyspark.sql import Row

all_files = getFiles(csv_path)

target_cols = sqlContext.table("taxi").dtypes

# Loop through all of the CSV files
for f in all_files:
  print("Processing file {}".format(f))
  
  taxi_type = f.path.split('/')[-2]
  
  # Use Spark to read the current CSV file into a dataframe and add some new columns
  raw_df = spark\
            .read.option("header", "true")\
            .csv(f.path)\
            .withColumn("filename",  pyf.input_file_name())\
            .withColumn("taxi_type", pyf.lit(taxi_type))
  
  # Remove spaces from column names and convert to lowercase
  for n in raw_df.schema.names:
    clean_name = n.strip().lower()
    if n != clean_name:
      raw_df = raw_df.withColumnRenamed(n, clean_name)
  
  # Convert columns to standard names
  if "dolocationid"          in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("dolocationid", "dropoff_location_id")
  if "end_lat"               in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("end_lat", "dropoff_latitude")
  if "end_lon"               in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("end_lon", "dropoff_longitude")
  if "fare_amt"              in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("fare_amt", "fare_amount")
  if "improvement_surcharge" in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("improvement_surcharge", "surcharge")
  if "locationid"            in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("locationid", "pickup_location_id")
  if "lpep_dropoff_datetime" in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
  if "lpep_pickup_datetime"  in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
  if "pickup_date"           in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("pickup_date", "pickup_datetime")
  if "pulocationid"          in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("pulocationid", "pickup_location_id")
  if "ratecodeid"            in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("ratecodeid", "rate_code")
  if "sr_flag"               in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("sr_flag", "shared_ride")
  if "start_lat"             in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("start_lat", "pickup_latitude")
  if "start_lon"             in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("start_lon", "pickup_longitude")
  if "store_and_fwd_flag"    in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("store_and_fwd_flag", "store_and_forward")
  if "tip_amt"               in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("tip_amt", "tip_amount")
  if "tolls_amt"             in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("tolls_amt", "tolls_amount")
  if "total_amt"             in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("total_amt", "total_amount")
  if "tpep_dropoff_datetime" in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
  if "tpep_pickup_datetime"  in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
  if "trip_dropoff_datetime" in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("trip_dropoff_datetime", "dropoff_datetime")
  if "trip_pickup_datetime"  in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("trip_pickup_datetime", "pickup_datetime")
  if "vendorid"              in raw_df.schema.names: raw_df = raw_df.withColumnRenamed("vendorid", "vendor_id")

  # Parse datetime columns to a timestamp
  for dt in raw_df.dtypes:
    if dt[0].endswith("datetime"):
      raw_df = raw_df.withColumn(dt[0], pyf.to_timestamp(dt[0], 'yyyy-MM-dd HH:mm:ss'))
  
  # Add a "year" column
  raw_df = raw_df.withColumn("year", pyf.year("pickup_datetime"))
  
  # Add missing columns
  for tc in target_cols:
    if tc[0] not in raw_df.schema.names:
      raw_df = raw_df.withColumn(tc[0], pyf.lit(None))

  # Make columns datatype match target
  for tc in target_cols:
    ac = next(x for x in raw_df.dtypes if x[0] == tc[0])
    if ac[1] != tc[1]:
      raw_df = raw_df.withColumn(ac[0], raw_df[ac[0]].cast(tc[1]))

  # Reorder columns to match target
  raw_df = raw_df.select([x[0] for x in target_cols])

  raw_df.write.insertInto("taxi", overwrite = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we loaded the CSV data into our table with multiple writes, we will have many small Parquet files.  Use Databricks Delta functionality to clean up those files and optimize
# MAGIC the file layout.

# COMMAND ----------

spark.sql("OPTIMIZE taxi ZORDER BY (pickup_datetime)")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", 'false')
spark.sql("VACUUM taxi RETAIN 0 hours")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC We're all done!  Now we just need to check our work to make sure everything looks good.
# MAGIC 
# MAGIC First, read all of the CSV files at once and count the rows in them.  This will let us check below to make sure we got all of the data.

# COMMAND ----------

csv_df = spark.read.option("header", "true").csv("{}/*/*.csv".format(csv_path))
"{:,}".format(csv_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Now create a nice matrix that shows the number of rows by `year` and `taxi_data`.  Add a column that shows the total rows for each year and a summary row at the bottom that shows the
# MAGIC number of rows for each taxi type.  The grand total in the bottom-right cell should match the total number of rows we read from CSV in the command above.

# COMMAND ----------

import pyspark.sql.functions as pyf

# Read the data that we read as parquet (even though it's actually a Delta table)
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
parq_df = spark.read.parquet(parquet_path)
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "true")

# Summarize the row counts by year and taxi_type
summary_df = parq_df\
              .withColumn("year", pyf.col("year").cast("string"))\
              .groupBy("year")\
              .pivot("taxi_type")\
              .count()\
              .fillna(0)\
              .orderBy("year")

# Add a "total" column that shows the total counts for each year
summary_df = summary_df.withColumn('total', sum(summary_df[col] for col in summary_df.columns if col != "year"))

# Add a summary row that shows the total counts for each column
exprs = [pyf.sum(x).alias(x) for x in summary_df.columns if x != "year"]
summary_row = summary_df.agg(*exprs).selectExpr("'total' AS year", "*")
summary_df = summary_df.union(summary_row)

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Just for fun, add a visualization that shows the rowcounts by year.  This lets us see that yellow taxi rides are declining over time while FHV rides are exploding.

# COMMAND ----------

display(
  parq_df\
    .filter("year BETWEEN 2009 AND 2018")\
    .groupBy("year")\
    .pivot("taxi_type")\
    .count()\
    .orderBy("year")
)
