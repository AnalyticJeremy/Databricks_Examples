<img src="https://community.cloud.databricks.com/login/databricks_logoTM_rgb_TM.svg" width="400" />

# Databricks Examples
This repository contains a collection of notebooks demonstrating various features in Azure Databricks.

**[Working With Pandas](https://analyticjeremy.github.io/Databricks_Examples/Working%20With%20Pandas.html)**:
a notebook demonstrating the `pandas_udf` feature in Spark 2.3, which allows you to distribute processing of pandas
dataframes across a cluster

**[Plotting Distributions](https://analyticjeremy.github.io/Databricks_Examples/Plotting%20Distributions.html)**:
a notebook demonstrating how to plot the distribution of all numeric columns in a Spark dataframe using `matplotlib`

**[Write to a Single CSV File](https://analyticjeremy.github.io/Databricks_Examples/Write%20to%20a%20Single%20CSV%20File)**:
if you have a small dataset in Spark, you can write the data into a single CSV file (instead of Spark's default behavior of
writing to multiple files)

**NYC Taxi Data**: a pair of notebooks for downloading the [NYC Taxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
datasets as CSV files and converting those CSV files to a single Parquet dataset

- [Notebook 1: Download CSV Files](https://analyticjeremy.github.io/Databricks_Examples/NYC%20Taxi%20Data/01%20Download%20CSV%20Files.html)
- [Notebook 2: Convert CSV to Parquet](https://analyticjeremy.github.io/Databricks_Examples/NYC%20Taxi%20Data/02%20Convert%20CSV%20to%20Parquet.html)
