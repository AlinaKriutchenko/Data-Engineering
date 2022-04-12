# Pipeline set up (To Bronze, to Silver Layers)


**Task name**: is the name of the job.<br/>
**Type**: selected notebook that will be running <br/>
**Cluster**: With the newest databricks updates, every task in the pipeline can run on the same cluster.

### A job is created for every databricks notebook. <br/> Data Ingestion from the Django API endpoint (Landing to Bronze) example:


<img width="800" alt="1" src="https://user-images.githubusercontent.com/65950685/160756471-e37f8eea-abb7-4897-a680-2c442e00292f.png">

The bronze layer contains raw data.

## Schedule a job

**Schedule Type**: Scheduled <br/>
**Schedule, Every**: Day <br/>
**At**: 17:00 <br/>
**Timezone**: UTC+11:00 

The scheduled job provides daily ingestion of the new data.

<img width="800" alt="schedule" src="https://user-images.githubusercontent.com/65950685/160787929-d29937a3-ea4b-4c67-bbf7-11faaaf8f002.png">


## Set up email alerts if the job fails

**Alerts**: Enter your email <br/>
**Failure**: ✓

In case of failed job, the notification will be sent to your email. <br/> This provides a possibility to troubleshoot it in a timely manner.

<img width="800" alt="email alerts" src="https://user-images.githubusercontent.com/65950685/160783607-e396ac22-b558-41df-a691-b9766b87c380.png">


### Pipeline structure

Following the Medallion architecture, the data loaded to bronze using **API endpoint** (Venue-Landing-To-Bronze) and from **s3 bucket** using mount source (Track-Landing-To-Bronze). <br/> The **Track-Bronze-To-Silver** the merged the data from the 2 Bronze level and prepare it for insight (Gold level).

<img width="800" alt="pipeline_run" src="https://user-images.githubusercontent.com/65950685/160775879-bc6b2116-aceb-4cb6-91b5-2cd8ecaf5d69.png">

