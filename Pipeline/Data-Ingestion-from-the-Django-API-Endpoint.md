## Data Ingestion from the Django API endpoint for daily scheduled job (Venue-Landing-To-Bronze)

A token is used to create an authenticated request to get venue data from API endpoint. <br/>
An upsert workflow: an API request to a public API, storing the data. <br/>
A scheduled job for daily updates of records.



### Import Libraries

```
import requests
import json
from pyspark.sql import functions as F
```

### To set up Timezone for datetime specific parameters.

```
spark.conf.set("spark.sql.session.timeZone", "Australia/Melbourne")
```

### A loop to get an url from the Django API endpoint.

The authorization token required. <br/> Every url contains a single record. The following url receives by using 'next'. <br/> The 'next' is a method that returns a prepared request object for the next request in a redirection.

https://www.w3schools.com/python/ref_requests_response.asp <br/>
https://docs.python-requests.org/en/latest/
```
def get_data(url):
  result =[]
  while url is not None:
      headers = {
        'content-type': 'application/json',
        'authorization': 'Token ****************************************'
      }     
      response = requests.get(url=url, headers=headers).json()
      url = response.get('next')
      result.extend(response.get('results'))
  return result
```

### Pull all the data from the API endpoint
* Pull from venue
* Pull from venue status
* Pull from account
```
# Venue Url
venue_url = "https://****-***.***.**.****.***/*****/?limit=100&offset=0"
venue = get_data(venue_url)

# Venue_status
status_url = "https://****-***.***.**.****.***/*****/*****/?limit=100&offset=0"
status = get_data(status_url)

# Account
account_url = "https://****-***.***.**.****.***/****/*******/?limit=100&offset=0"
account = get_data(account_url)
```


%md
### Reading spark dataframe
Adding `date` column to venue_df <br />
Renaming `name` columns to avoid duplication in status_df and account_df

https://spark.apache.org/docs/latest/sql-data-sources-json.html
```
# venue
venue_df = spark.read.json(sc.parallelize([json.dumps(v) for v in venue]))
venue_df = venue_df.withColumn('date', F.current_date())

# venue_status
status_df = spark.read.json(sc.parallelize([json.dumps(row) for row in status]))
status_df = status_df.withColumnRenamed("name", 'venue_status_name') 

# account
account_df = spark.read.json(sc.parallelize([json.dumps(row) for row in account]))
account_df = account_df.withColumnRenamed("name", 'account_name') 
```

### Union of venue_df and account_df by id

https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.join.html

```
output_df = (venue_df
             .join(status_df, venue_df["venue_status_id"] == status_df["id"])
             .join(account_df, venue_df["account_id"] == account_df["id"]) 
             .select(venue_df['*'], status_df.venue_status_name, account_df.account_name))           
```
### The result of the first data load and a target for future data loads

```
## Run this only once
# output_df.write.saveAsTable('intern.tbl_bronze_venue')
```

### Registering a temp table (source) for new data

The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame.

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.registerTempTable.html

```
output_df.registerTempTable('tmp_source_venue')
```
 
### Merge source (new data) into target (existing data). An upsert workflow.
https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html
```
%sql
merge into intern.tbl_bronze_venue target
  using tmp_source_venue source
    on source.id = target.id and source.date = target.date
when matched
  then update set *
when not matched
  then insert *  
```

### The notebook is ready for a scheduled job.
