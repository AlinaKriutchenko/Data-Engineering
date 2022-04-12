# Databricks Notebook. Predicthq-Bronze-To-Silver-Unified (PredictHQ)

A single unified events dataset where only unique active events are kept. Grouping by recurring and single events. <br/>
Duplicates removal.


### Import function module

https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#module-pyspark.sql.functions

```
%python
from pyspark.sql import functions
```

#### Register 'event-group' table with 'is_recurring' and 'is_recurring' values


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

#### We want to have non-duplicated events by tracking the `updated` flag and only keep the latest updated information
```
create or replace temporary view tmp_deleted_event as (
  select id
  from predicthq.tbl_bronze_events
  where state = 'deleted'
);
```
#### Create a temporary view
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
#### Remove deleted events in updated table
```
delete from predicthq.tbl_silver_events tgt
where tgt.id in (
  select id
  from tmp_deleted_event
);
```
#### Optimize
```
optimize predicthq.tbl_silver_events
zorder by updated, id;
```
