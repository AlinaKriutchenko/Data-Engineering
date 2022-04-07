## Structured streaming with Autoloader from AWS S3 (Track-landing-to-bronze)

An ingestion of the data from s3 bucket to bronze layer in development environment. 

### A Function to mount the data


```
def mount_s3_bucket(src_s3_bucket: str, 
                    mount_location: str) -> None:
  """Optionally mounts the s3 bucket and specify the location of the mount

  Parameters:
  src_s3_bucket (str): full path of the s3 source bucket
  mount_location (str): full path on dbfs that was mounted on
  """
  for mount in dbutils.fs.mounts():
      if mount.mountPoint == mount_location:
        print('Directory has been mounted:')
        print(f'source: {mount.source}')
        print(f'mount : {mount.mountPoint}')
        return
  dbutils.fs.mount(f's3a://{src_s3_bucket}', mount_location)
  display(dbutils.fs.ls(mount_location)) 
```


### To do the mounting you need:
* aws_bucket_name
* mount_location

```
aws_bucket_name = '****-****-*****-******-************-**-*********-*-***'
mount_location = '/mnt/*****/'

mount_s3_bucket(aws_bucket_name, mount_location)
```

### Schema hints to change datatype before streaming
Schema hints used to change data type.
```
schema_hints = """
  id bigint
  , date1 timestamp
  , date2 timestamp
"""
```

### To remove checkpoint_path if table need to be deleted and loaded again

```
#dbutils.fs.rm(checkpoint_path, recurse=True)
```

### Structured Streaming with Autoloader into the table

https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html

```
checkpoint_path = '/mnt/******/_checkpoints/****/*****/bronze'
path = '/mnt/*****/2022'
dest_table = 'intern.tbl_bronze_*****'

df = (spark.readStream.format('cloudFiles')
           .option('cloudFiles.format', 'json') 
           # The schema location directory keeps track of your data schema over time
           .option('cloudFiles.schemaHints', schema_hints)
           .option('cloudFiles.schemaLocation', checkpoint_path) 
           .option('recursiveFileLookup', 'true')
           .load(path))
  
(df.writeStream 
   .option('mergeSchemav', 'true') 
   .option('checkpointLocation", checkpoint_path) 
   .option('location', '/mnt/data/****/*****')
   .trigger(once=True)
   .table(dest_table))
```


### In case there is a need to unmount:
```
#def sub_unmount(str_path):
#    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
#        dbutils.fs.unmount(str_path)
#
#sub_unmount('/mnt/track')
```
