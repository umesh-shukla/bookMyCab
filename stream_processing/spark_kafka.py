## Spark streaming Code ###
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import psycopg2
from pyspark.sql import Row, SQLContext 
import json 
import redis
from custom_packages.elasticWrapper import ElasticWrapper
__all__ = ["SparkSession"]

# Parses elasticsearch query result and returns records and their IDs
def convert2Json(result): 
    jsonResult = json.dumps(result)
    #print jsonResult
    records = []
    ids = []
    for k, v in result.items(): 
        #print item['cab_location'] + ", " + item['cust_location']
        if str(k) == 'hits':
            for item in v['hits']:
                records.append(item['_source'])
                ids.append(item['_id'])
        #print "Key:" + str(k) +", "+"Value:"+str(v['hits'])
    return (records, ids)

# function to parse line of csv data into Sensor class
def parseSensor(item):
    p = item.split(",")
    print type(p)
    ret_val ={'ID': p[0], 'VendorID':
p[1],'tpep_pickup_datetime':p[2],'tpep_dropoff_datetime':p[3],'passenger_count':p[4],'trip_distance':p[5],'pickup_longitude':p[6],'pickup_latitude':p[7],'RatecodeID':p[8],'store_and_fwd_flag':p[9],'dropoff_longitude':p[10],'dropoff_latitude':p[11],'payment_type':p[12],'fare_amount':p[13],'extra':p[14],'mta_tax':p[15],'tip_amount':p[16],'tolls_amount':p[17],'improvement_surcharge':p[18],'total_amount':p[18]}
    return ret_val


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']
   
## Main Loop ## 
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>")
        exit(-1)

    #conn = psycopg2.connect(database="testdb", user="postgres", password="pass", host="ip-172-31-2-14", port="5432")
                
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 3)  # window size in sec 
    zkQuorum, topic = sys.argv[1:]
    zkQuorum = "localhost::2181"
    customer_topic = "customer-request"
    driver_topic = "driver-request"
    kafkaBrokers = {"metadata.broker.list": "ec2-52-43-129-33.us-west-2.compute.amazonaws.com:9092, ec2-52-26-161-99.us-west-2.compute.amazonaws.com:9092, ec2-52-89-109-57.us-west-2.compute.amazonaws.com:9092, ec2-54-69-9-16.us-west-2.compute.amazonaws.com:9092"}
    ew_cust = ElasticWrapper('cust')
    #ew = ElasticWrapper()
    ## Driver Stream Processing ## 
    driverStream = KafkaUtils.createDirectStream(ssc, [driver_topic], kafkaBrokers)
    cabStreamData = driverStream.map(lambda x: x[1])
    driverDataRDD = cabStreamData.map(lambda x: parseSensor(x))
    drivers = driverDataRDD.map(lambda item: {'cabID': item['ID'], 'lat' :item['pickup_latitude'],'long' : item['pickup_longitude'] }) 

    # Add driver's entry to database
    def addDrivertoDB(driverRequest):
        print driverRequest
        ew_cab = ElasticWrapper('cab')
        for driverData in driverRequest: 
            print driverData
            cabData = {'cab_location': {'lat': driverData['lat'], 'lon': driverData['long']},  'cab_id': driverData['cabID']}
            ew_cab.create_document_multi([cabData])
    drivers.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: addDrivertoDB(x)))

    # Customer Data Stream Handing 
    customerStream = KafkaUtils.createDirectStream(ssc, [customer_topic], kafkaBrokers)
    customerStreamData = customerStream.map(lambda x: x[1])
    customerDataRDD = customerStreamData.map(lambda x: parseSensor(x))
    customers = customerDataRDD.map(lambda item: {'custID': item['ID'], 'lat' : item['pickup_latitude'],'long' :item['pickup_longitude'] , 'dropoff_lat': item['dropoff_latitude'], 'dropoff_long': item['dropoff_longitude']})
    customers.pprint()

    # Find closest driver for each RDD and store it to DB 
    def saveCabAssignmentToDb(customer, closestDriver):
        ewCabAssignDb = ElasticWrapper('cab_assign')
        cabAssignData = {'cust_id':customer['custID'], 'cust_pickup_location':{'lat':customer['lat'],'lon':customer['long']} , 'cust_dropoff_location': {'lat':customer['dropoff_lat'],'lon':customer['dropoff_long']} , 'cab_id':closestDriver['cab_id'] ,'cab_location':closestDriver['cab_location'] }  
        print cabAssignData
        ewCabAssignDb.create_document_multi([cabAssignData])
    
    # Find closest driver, given customer lat, lon
    def getClosestDriver(custRequest):
        ew_cab = ElasticWrapper('cab')
        for customer in custRequest:
            print customer 
            cab_query = {'sort': [{'_geo_distance': {'cab_location': {'lat': customer['lat'], 'lon': customer['long']}, 'order': 'asc','unit':'m'}}],  "query": { "match_all": {} } }
            result = ew_cab.search_document(cab_query)
            (closestDrivers, doc_ids) = convert2Json(result)
            print "Customer: "+ str(customer['custID']) +"Assigned driver: "+ str(closestDrivers[0]['cab_id']) 
            # Store custID, cabID and their lat, lons in separate DB and then delete cabID from driver DB now
            saveCabAssignmentToDb(customer, closestDrivers[0])
            # delete assigned Cab record from cab database
            ew_cab.delete_doc_by_index(doc_ids[0])
            
    customers.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: getClosestDriver(x)))    
    ssc.start()
    ssc.awaitTermination()

