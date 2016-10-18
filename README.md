# ASAP Cab

## Table of contents
1. [Introduction](README.md#introduction)
2. [Data Pipeline & Architecture](README.md#data-pipeline)  
3. [AWS Clusters](README.md#aws-clusters) 
4. [Performance](README.md#performance)

## Introduction 
[Back to Table of contents](README.md#table-of-contents)

ASAP Cab is a real-time scalable data platform aka Uber system for Yellow/Green Cabs. This can be used by any taxi company to do rider reservations and accounts of cab driver locations in real-time. The system assigns the geographically closest cab to a rider to minimize customer wait time. Refer to asapcab_slides.pdf for more details. 

## Data Pipeline & Architecture
[Back to Table of contents](README.md#table-of-contents)

Below diagram shows data pipeline of this project. 

![Alt text](front_end/static/data_pipeline.png?raw=true “Data Pipeline“)

### Data Ingestion
Rider data is comes from front end UI which essentially comes due to riders requesting to reserve a cab. Cab drivers data comes from cab as an independent stream. This stream is simulated in current project by using NYC yellow cab data. Both streams: Rider-request and cab driver-request are ingested into Kafka using two Kafka topics. Kafka feeds these two streams into Spark streaming engine.  

### Spark Streaming 
Doing rider-cab assignment among a group of riders and a group of cabs is essentially a matching problem which can be solved by taking a cartesian of two streams. A better way to  compute the distance between cabs and riders would be to store cabs in a database and query closest cabs for a given rider. Spark streaming engine uses this logic to compute assignments. 

### Databases 
Two databases are used: Elasticsearch and Redis. Elasticsearch is used for two purposes: To make geo-spatial queries to find the closest cab and to store final rider-cab assignments which is shown on the front end. Redis is used as a cache layer to avoid any cab overbooking problem which can happen in this system due to distributed nature. 

## AWS Clusters
[Back to Table of contents](README.md#table-of-contents)

This pipeline is running on 4-node m4.large AWS cluster for now. Other than Redis and Flask app, all other components are running on all 4 nodes. This pipeline will be scaled to 11-node cluster in future. 
 
## Performance
[Back to Table of contents](README.md#table-of-contents)

The system can handle ~ 900 requests (riders + cab drivers) per second with 4-node AWS cluster which is decent. The system was stress tested with NYC yellow cab data and below is the performance chart w.r.t. no. of users which suggests that system is stable with micro-batch window size of 5 sec. 

![Alt text](front_end/static/performance.png?raw=true)



 



