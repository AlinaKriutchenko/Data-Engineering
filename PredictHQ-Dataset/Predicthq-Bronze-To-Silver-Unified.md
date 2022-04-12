# A creation of Predicthq-Bronze-To-Silver-Unified from PredictHQ data.

* Duplicates removed
* Only active events a kept
* Merging new data updates
* Removing deleted events from old records
* Adding an 'event-group' column for single (true) and recurring events (false)

### Import function module

https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#module-pyspark.sql.functions

```
%python
from pyspark.sql import functions
```

#### Addition of an 'event-group' column:
* where the value of entity in **entities** column is 'recurring' return true
* otherwise return 'false'

```
%python
@functions.udf('boolean')
def is_recurring(entities):
  for entity in entities:
    if entity['type'] == 'event-group':
      return True
  return False

spark.udf.register('is_recurring', is_recurring)
```

#### Filtering non-duplicated events by tracking the updated flag and only keep the latest updated information.
Select id where the state is 'deleted'

```
create or replace temporary view tmp_deleted_event as (
  select id
  from predicthq.tbl_bronze_events
  where state = 'deleted'
);
```
#### Creation of the temporary view:
* 'tmp_casted_event' from 'predicthq.tbl_bronze_events' where 'id' not in 'tmp_deleted_event'.
* cast(end as timestamp) selects the last unique timestamp records and removes other duplicates.
* change the data type from json for columns: entities, geo, labels, location, place_hierarchies, start
* creation of the new column 'row_num' by adding a row number to 'id'. It will be used to merge data later by unique id, since some recurring events have the same id.
```
create or replace temporary view tmp_casted_event as (
  select 
    id
    , cast(aviation_rank as int) as aviation_rank
    , brand_safe
    , category
    , country
    , description
    , cast(duration as int) as duration
    , cast(end as timestamp) as end
    , from_json(entities, 'array<struct<
        entity_id: string,
        name: string,
        type: string,
        category: string,
        labels: array<string>>>') as entities
    , cast(first_seen as timestamp) as first_seen
    , from_json(geo, 'struct<
        geometry: struct<type: string, coordinates: array<double>>>') as geo
    , from_json(labels, 'array<string>') as labels
    , local_rank
    , from_json(location, 'array<double>') as location  --lng, lat arrangement
    , phq_attendance
    , from_json(place_hierarchies, 'array<array<string>>') as place_hierarchies
    , private
    , cast(rank as int) as rank
    , relevance
    , scope
    , to_date(from_utc_timestamp(cast(start as timestamp), timezone), 'yyyyMMdd') as local_date
    , cast(start as timestamp) as start
    , state
    , timezone
    , title
    , cast(updated as timestamp) as updated
    , parent_event
    , predicted_end
    , cancelled
    , deleted_reason
    , duplicate_of_id
    , row_number() over(partition by id order by updated desc) as row_num
  from predicthq.tbl_bronze_events
  where id not in (
    select id
    from tmp_deleted_event
  )
);
```
#### Creation of the temporary view:
* 'tmp_unified_events' from 'tmp_casted_event' where 'id' is not null and 'row_num' is 1

#### Create
```

create or replace temporary view tmp_unified_events as (
  select 
      id
    , aviation_rank
    , brand_safe
    , category
    , country
    , description
    , duration
    , end
    , to_json(entities) as entities
    , first_seen
    , geo
    , labels
    , local_rank
    , location
    , phq_attendance
    , place_hierarchies
    , private
    , rank
    , relevance
    , scope
    , start
    , state
    , timezone
    , title
    , updated
    , parent_event
    , predicted_end
    , cancelled
    , deleted_reason
    , duplicate_of_id
    , is_recurring(entities) as recurring
  from tmp_casted_event
  where id is not null
  and row_num = 1
);
```
#### One-off table creation

```
# Run only once
# create or replace table predicthq.tbl_silver_events as (
#   select *
#   from tmp_unified_events
# );
```
#### Merging new events with old ones
```
merge into predicthq.tbl_silver_events target
  using tmp_unified_events source
    on (source.id = target.id)
when matched
  then update set *
when not matched
  then insert *;
```
#### Removing deleted events in the updated table
```
delete from predicthq.tbl_silver_events tgt
where tgt.id in (
  select id
  from tmp_deleted_event
);
```
#### Optimizing
```
optimize predicthq.tbl_silver_events
zorder by updated, id;
```
