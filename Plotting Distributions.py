# Databricks notebook source
# MAGIC %md
# MAGIC # Plotting Distributions in Databricks
# MAGIC <img src='https://matplotlib.org/_static/logo2.png' />
# MAGIC 
# MAGIC Databricks is a powerful tool for exploring and analyzing data.  When you first open a new dataset, one of the first things you may want to
# MAGIC understand is the distribution of numerical variables.  This resuable code shows how you can quickly view that data.

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

# MAGIC %md
# MAGIC Loop through all of the columns in our dataframe and identify the ones with a numeric data type.

# COMMAND ----------

numeric_columns = []
numeric_types = ['double', 'float', 'int', 'long', 'short']

for x in df.dtypes:
  if x[1] in numeric_types:
    numeric_columns.append(x[0])
    
numeric_columns

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell does the real work here.  It declares a function that accepts a Spark dataframe and the names of the columns for which we wish to plot the distribution.
# MAGIC Plotting for a *really* big dataset would take a long time (and possibly crash the driver node) so, when necessary, we sample only 1,000,000 rows from the dataframe
# MAGIC to use in our plots.
# MAGIC 
# MAGIC We use the "[seaborn](https://seaborn.pydata.org/)" package to create a plot of the distribution.  It includes both a histogram of the values and a plot of the
# MAGIC probability distribution curve.  We use the underlying "matplotlib" library's functionality to augment the histogram with vertical lines showing the mean and
# MAGIC one standard deviation from the mean.

# COMMAND ----------

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def plot_distributions(df, cols):
  row_count = df.count()
  target_size = 1000000
  
  num_df = df.select(cols)
  
  # If the input dataframe is super big, sample the rows to a more manageable size.
  if row_count > 2 * target_size:
    fraction = target_size / row_count
    sampled_df = num_df.sample(False, fraction)
  else:
    sampled_df = num_df
  
  sampled_df.cache()
  
  plot_count = len(cols)
  counter = 1
  
  fig = plt.figure()

  for col in cols:
    col_name = cols[counter - 1]
    values = np.array(sampled_df.select(col_name).collect())

    # Let Seaborn plot the distribution
    ax = plt.subplot(plot_count, 1, counter)
    ax = sns.distplot(values)

    # Add vertical lines showing the mean and +/- 1 standard deviation
    mean = np.mean(values)
    sigma1, sigma2 = mean - np.std(values), mean + np.std(values)
    min, max = np.min(values), np.max(values)
    padding = (max - min) * 0.005

    ax.axvline(mean, color='#c00000', linestyle='dashed', linewidth=1)
    ax.annotate("$\mu$={:,.1f}".format(mean), xy=(mean + padding, 0), ha='left', fontsize=10)

    ax.axvline(sigma1, color='#202020', linestyle='dashed', linewidth=0.5, alpha=0.37)
    ax.axvline(sigma2, color='#202020', linestyle='dashed', linewidth=0.5, alpha=0.37)

    ax.set_title("Distribution  of  \"{}\"".format(col_name), {'fontsize': '20'})
    ax.set_ylabel('probability')
    
    counter += 1

  fig.suptitle("Distribution of Numeric Values in a Spark Dataframe", fontsize=35)
  fig.set_dpi(96)
  fig.set_size_inches(18, 6.5 * plot_count)
  
  if plot_count == 1:
    plt.subplots_adjust(top = 0.8)
  else:
    plt.subplots_adjust(hspace=0.5)

  return(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Now just pass our dataframe to the function, and display the results!

# COMMAND ----------

fig = plot_distributions(df, numeric_columns)
display(fig)

# COMMAND ----------


