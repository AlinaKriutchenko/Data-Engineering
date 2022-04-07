# Databricks Notebook. track-bronze-to-silver

Merge venue information for account id, customer store location, business name to bronze track


### Import
```
from pyspark.sql.functions import col
```


### 


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

### temp_track from db_name.tbl_bronze_track
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

display(track_bronze)
display(venue_bronze)

display(venue_bronze.groupBy('id').count())
```


### Add column from venue_bronze to venue_track:
business_name, customer_store_id, account_id,  account_name)
```
silver_track = (track_bronze
             .join(venue_bronze, track_bronze["venue_id"] == venue_bronze["id"])             
             .select(track_bronze['*'], venue_bronze.business_name, venue_bronze.customer_store_id, venue_bronze.account_id, venue_bronze.account_name))
display(silver_track)
```

### Create 
db_name.tbl_silver_track
```
# Run this only once
#silver_track.write.saveAsTable('db_name.tbl_silver_track')
#track_bronze.write.mode("overwrite").saveAsTable("db_name.tbl_silver_track")
```

### Merge new data into
db_name.tbl_silver_track
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
