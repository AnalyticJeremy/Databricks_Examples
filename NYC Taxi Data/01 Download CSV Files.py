# Databricks notebook source
# MAGIC %md
# MAGIC # Downloading NYC Taxi Data
# MAGIC 
# MAGIC When working on a Spark project, it's often handy to have a really big dataset to work with.  One very popular, very large dataset is the
# MAGIC [NYC Taxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset.  This data is provided by the New York City Taxi & Limousine Commission.  It contains data
# MAGIC on individual taxi trips taken between 2009 and 2018.  The data is provided as CSV files and is stored in a public Amazon S3 bucket.
# MAGIC 
# MAGIC To use the data in Spark, you will need to download it to your own HDFS-compliant storage.  The script below assumes that you've mounted your target storage location in the DBFS.

# COMMAND ----------

# The location where you've mounted the target storage
# IMPORTANT: This script will be accessing the files using native Python libraries, NOT Spark libraries.  Therefore, the path must start with "/dbfs/"
output_path = "/dbfs/mnt/taxi/csv"

# The most recent month for which data is available.  (Check the web site linked above to see if new data has been made available.)
max_month = "2018-06"

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we build a Python list of all possible year/month combinations.  We'll filter this down to the exact range of dates that we need later.

# COMMAND ----------

all_months = []
for y in range(2000, 2050):
  for m in range(1, 13):
    month = "{0}-{1:02d}".format(y, m)
    all_months.append(month)

# COMMAND ----------

# MAGIC %md
# MAGIC The NYC Taxi dataset is divided by taxi type:
# MAGIC  - "yellow" taxicabs can be hailed by people on the street
# MAGIC  - "fhv" (or "For Hire Vehicles") are ride-sharing services, black car services, or luxury limo services.  Rides in these vehicles must be pre-arranged with a dispatching service.  They cannot pickup street hails.
# MAGIC  - "green" taxicabs were introduced in 2013.  They are a hybrid between yellow taxis and FHV's.  They can accept street hails in certain parts of the city that are often under-served by yellow taxis.  They can also be pre-arranged by a dispatching service.
# MAGIC 
# MAGIC Each taxi type has a different schema and covers a different range of months.  For example, data for yellow taxis goes back to 2009, but FHV data is only provided from 2015 to the present.
# MAGIC 
# MAGIC Below, we declare the types of taxis and the date ranges for which their files are available.  If a new type of taxi data becomes available, just add it here, and the rest of
# MAGIC this script should pick it up.

# COMMAND ----------

taxi_types = {
  'yellow': ("2009-01", max_month),
  'green':  ("2013-08", max_month),
  'fhv':    ("2015-01", max_month)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downloading the Data
# MAGIC 
# MAGIC Using Python, we now loop through the different types of taxis and download each type's monthly CSV files.

# COMMAND ----------

import urllib.request
import os

for k in taxi_types.keys():
  print("Processing \"{0}\"  ({1}  through  {2})".format(k, taxi_types[k][0], taxi_types[k][1]))
  months = [x for x in all_months if x >= taxi_types[k][0] and x <= taxi_types[k][1]]
  
  type_path = "{0}/{1}".format(output_path, k)
  if not os.path.exists(type_path):
    os.makedirs(type_path)
  
  for m in months:
    url = "https://s3.amazonaws.com/nyc-tlc/trip+data/{0}_tripdata_{1}.csv".format(k, m)
    filename = "{0}/{1}.csv".format(type_path, m)

    # Do not download the file if we already have a copy of it
    if not os.path.exists(filename):
      urllib.request.urlretrieve(url, filename)
      print("   Downloaded {}".format(m))
    else:
      print("   Skipped {} (file already exists)".format(m))
    
  print("============================================================")
