<img src="https://community.cloud.databricks.com/login/databricks_logoTM_rgb_TM.svg" width="400" />

# Databricks Examples
This repository contains a collection of notebooks demonstrating various features in Azure Databricks.

**[Working With Pandas](Working%20With%20Pandas.ipynb)**:
a notebook demonstrating the `pandas_udf` feature in Spark 2.3, which allows you to distribute processing of pandas
dataframes across a cluster

**[Plotting Distributions](Plotting%20Distributions.ipynb)**:
a notebook demonstrating how to plot the distribution of all numeric columns in a Spark dataframe using `matplotlib`

**[Write to a Single CSV File](Write%20to%20a%20Single%20CSV%20File)**:
if you have a small dataset in Spark, you can write the data into a single CSV file (instead of Spark's default behavior of
writing to multiple files)

**NYC Taxi Data**: Do you need a big dataset for experimenting with Spark?  The
[NYC Taxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is free and publicly available.
This pair of notebooks will download all of the raw data and then convert it into a Delta table.
- Notebook 1: [Download Raw Parquet Files](NYC%20Taxi%20Data/01%20Download%20Raw%20Parquet%20Files.ipynb)
- Notebook 2: [Convert Raw Parquet to Delta](NYC%20Taxi%20Data/02%20Convert%20Raw%20Parquet%20to%20Delta.ipynb)

**[Custom Delimiter](Custom%20Delimiter.ipynb)**: a brief example showing how you
can use Spark to read data from a flat file if it uses a non-standard delmitier

**[Stream to Kafka](Stream%20to%20Kafka.ipynb)**: an example of how you can use Spark Streaming to send data to Azure Event Hubs using the Kafka API

**[DBUtils in Parallel](DBUtils%20in%20Parallel.ipynb)**: a demonstration of the performance gains of using `dbutils`
in parallel on a cluster instead of runnig it only on the driver

**[UDF Speed Testing](UDF%20Speed%20Testing.ipynb)**: a comparision of the performance of Scala UDF's vs. Python UDF's
