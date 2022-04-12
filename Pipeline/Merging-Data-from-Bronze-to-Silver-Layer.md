# Track-bronze-to-silver. 

Merge venue information for account id, customer store location, business name to bronze track


### Import function
```
from pyspark.sql.functions import col
```


### First dataframe. Creation of the temporary view:
* Selection of four columns from **'db_name.tbl_bronze_venue'**: business_nam, customer_store_id, account_id and account_name as **temp_venue**.
* Group **temp_venue** by **id**.
```
%sql
create or replace temporary view temp_venue as (
    select id
      , last(business_name) as business_name 
      , last(customer_store_id) as customer_store_id
      , last(account_id) as account_id
      , last(account_name) as account_name
    from db_name.tbl_bronze_venue   
    group by id 
);
```

### Second Dataframe. Creation of the temporary view from **db_name.tbl_bronze_track** as temp_track where:
* **cast** is a function to change column datatype.
* **cast** timestamp column as a timestamp type.
* **cast** duration integer column a duration type.
```
%sql
create or replace temporary view temp_track as (
    select id,venue_id, track_id, action, env
      , cast(timestamp as long) as timestamp
      , date_added, name, label, artist, release, isrc, upc
      , cast(duration as int) as duration
      , playlist_id, playlist_name, player_id, source, date_played, artist_id, release_id
     from db_name.tbl_bronze_track
);

```

### Transform temp_venue and temp_track to spark dataframe
```
track_bronze = spark.read.table("temp_track")
venue_bronze = spark.read.table("temp_venue")
```


### Merging track_bronze and venue_bronze pyspark dataframes.
Joining 
* business_name, customer_store_id, account_id,  account_name columns from venue_bronze
* all columns from track_bronze
* join where venue_id from track_bronze matches the id from venue_bronze.
```
silver_track = (track_bronze
             .join(venue_bronze, track_bronze["venue_id"] == venue_bronze["id"])             
             .select(track_bronze['*'], venue_bronze.business_name, venue_bronze.customer_store_id, venue_bronze.account_id, venue_bronze.account_name))
```

### Create db_name.tbl_silver_track (target for future dumps)

* **write.saveAsTable** saves **silver_track** pyspark dataframe a temp table.
* **.write.mode("overwrite")** overwrites the table.
* The **db_name.tbl_silver_track** only created during the first notebook run and later used a target table, where new data is being dumped.

```
# Run this only once
#silver_track.write.saveAsTable('db_name.tbl_silver_track')
#silver_bronze.write.mode("overwrite").saveAsTable("db_name.tbl_silver_track")
```

### Merging the new data into db_name.tbl_silver_track
**tmp_merge** used as a target and specified separately:
* from **temp_track** all columns selected.
* from **temp_venue** four columns selected.
* the left join used for **temp_venue** where **id** in temp_venue mathes **venue_id** in temp_track.

Merging:
* The new data merged into db_name.tbl_silver_track as tgt.
* When the id is matched, the data updated. 
* When the id is unmatched (new), the new records are inserted.

```
%sql
with tmp_merge as (
  select temp_track.*
  , temp_venue.business_name
  , temp_venue.customer_store_id
  , temp_venue.account_id
  , temp_venue.account_name
  from temp_track 
  left join temp_venue
  on temp_venue.id = temp_track.venue_id
)

merge into db_name.tbl_silver_track as tgt
  using tmp_merge src
    on tgt.id = src.id 
when matched then
  update set *
when not matched then
  insert *
```
