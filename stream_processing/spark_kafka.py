## Spark streaming Code ###
import sys
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import psycopg2
from pyspark.sql import Row, SQLContext 
import json 
import redis
from custom_packages.elasticWrapper import ElasticWrapper
from datetime import datetime

__all__ = ["SparkSession"]

# Parses elasticsearch query result and returns records and their IDs
def convert2Json(result): 
    jsonResult = json.dumps(result)
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

# Parse line of csv data into Sensor class
def parseStream(item):
    p = item.split(",")
    ret_val ={'ID': p[0], 'VendorID':p[1],'tpep_pickup_datetime':p[2],'tpep_dropoff_datetime':p[3],\
    'passenger_count':p[4],'trip_distance':p[5],'pickup_longitude':p[6],'pickup_latitude':p[7], \
    'RatecodeID':p[8],'store_and_fwd_flag':p[9],'dropoff_longitude':p[10],'dropoff_latitude':p[11], \
    'payment_type':p[12],'fare_amount':p[13],'extra':p[14],'mta_tax':p[15],'tip_amount':p[16], \
    'tolls_amount':p[17],'improvement_surcharge':p[18],'total_amount':p[18]}
    return ret_val
   
def findClosestDriver(driverList): 
    redisDB = redis.StrictRedis(host=confParams['redis_host'],  port=confParams['redis_port'], db=confParams['cabDB'], \
              password=confParams['redis_pwd'])
    id_idx = 0
    for driver in driverList: 
        #print "Driver: "+ str(driver)
        if str(redisDB.get(driver['cab_id'])) == 'None': 
            #print "Selected Driver: "+ str(driver['cab_id']) + "id: "+ str(id_idx)
            redisDB.set(driver['cab_id'], 1)
            return (driver, id_idx)
        id_idx = id_idx + 1
    return ('None', -1)
        
# Add driver's entry to database
def addDrivertoDB(driverRequest):
    #print driverRequest
    ew_cab = ElasticWrapper('cab')
    cab_list = []
    for driverData in driverRequest: 
        #print driverData
        cabData = {'cab_location': {'lat': driverData['lat'], 'lon': driverData['long']},  'cab_id': driverData['cabID']}
        cab_list.append(cabData)

    ew_cab.create_document_multi(cab_list)

def clearRadisCache():
    redisDB = redis.StrictRedis(host=confParams['redis_host'],  port=confParams['redis_port'], db=confParams['cabDB'], \
         password=confParams['redis_pwd'])
    recordDB = redis.StrictRedis(host=confParams['redis_host'],  port=confParams['redis_port'], db=confParams['custReservRecord'], \
         password=confParams['redis_pwd'])
    redisDB.flushdb()
    recordDB.flushdb()

# Find closest driver for each RDD and store it to DB 
def saveCabAssignmentToDb(customer, closestDriver):
    cabAssignData = {'cust_id':customer['custID'],'cust_pickup_location':{'lat':customer['lat'],'lon':customer['long']}, \
                    'cust_dropoff_location': {'lat':customer['dropoff_lat'],'lon':customer['dropoff_long']},'cab_id':closestDriver['cab_id'], \
                    'cab_location':closestDriver['cab_location'] }  
    return cabAssignData

# Find closest driver, given customer lat, lon
def getClosestDriver(customerStream):
    ew_cab = ElasticWrapper('cab')
    ewCabAssignDb = ElasticWrapper('cab_assign')
    cabAssignmentList = []
    customers = []
    for customer in customerStream:
        #print customer
        customers.append(customer)

    res = ew_cab.get_closest_items_bulk_query(customers)
    for i in range(0, len(customers)): 
        #print res['responses'][i]
        (closestDrivers, doc_ids) = convert2Json(res['responses'][i])
        (selectedDriver, id_idx) = findClosestDriver(closestDrivers)
        if (selectedDriver != 'None'):
            print "Customer: "+ str(customers[i]['custID']) + "Assigned driver: "+ str(selectedDriver['cab_id']) 
            cabAssignData = saveCabAssignmentToDb(customers[i], selectedDriver)
            cabAssignmentList.append(cabAssignData)
            # This is for front end purposes. Temporary hack. Need a pub-sub model in redis as final solution
            redisDBAssignmentRecord = redis.StrictRedis(host=confParams['redis_host'],  port=confParams['redis_port'], db=confParams['custReservRecord'], \
                                      password=confParams['redis_pwd'])
            redisDBAssignmentRecord.set(str(customer['custID']), str(selectedDriver['cab_id']))
            # Delete cab from cab database. TODO: Do a bulk delete
            try: 
                ew_cab.delete_doc_by_index(doc_ids[id_idx])
            except Exception as e:
                print e
                pass
        else:
            print "No suitable driver found"

    ewCabAssignDb.create_document_multi(cabAssignmentList)

def getConfParams(configFile): 
    config = open(configFile, 'r')
    confParams = {}
    for line in config:
        if '#' not in line: 
            items = re.split('=| |\n', line)
            #items.remove('')
            filt_items = [x for x in items if x != ''] 
            #print filt_items
            if len(filt_items) >= 2:  
                confParams[str(filt_items[0])] = str(filt_items[1])
    
    #print confParams
    return confParams

## Main Loop ## 
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python spark_kafka.py config_file")
        exit(-1)
    
    configFile = sys.argv[1]
    confParams = getConfParams(configFile) 
    rs = redis.StrictRedis(host=confParams['redis_host'],  port=confParams['redis_port'], db=confParams['cabDB'], \
         password=confParams['redis_pwd'])
    sc = SparkContext(appName="BookMyCab")
    ssc = StreamingContext(sc, 2)  # window size in sec 
    zkQuorum = confParams['zkQuorum']
    customer_topic = confParams['customer_topic']
    driver_topic = confParams['driver_topic']
    kafkaBrokers = {"metadata.broker.list": confParams['kafkaBrokers']}
    ew_cust = ElasticWrapper('cust')

    ## Driver Stream Processing ## 
    driverStream = KafkaUtils.createDirectStream(ssc, [driver_topic], kafkaBrokers)
    cabStreamData = driverStream.map(lambda x: x[1])
    driverDataRDD = cabStreamData.map(lambda x: parseStream(x))
    drivers = driverDataRDD.map(lambda item: {'cabID': item['ID'], 'lat' :item['pickup_latitude'], \
             'long' : item['pickup_longitude'] }) 
    drivers.pprint()

    # Clear Radis Cache before handling any customers 
    drivers.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: clearRadisCache()))
    drivers.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: addDrivertoDB(x)))

    # Customer Data Stream Handing 
    customerStream = KafkaUtils.createDirectStream(ssc, [customer_topic], kafkaBrokers)
    customerStreamData = customerStream.map(lambda x: x[1])
    customerDataRDD = customerStreamData.map(lambda x: parseStream(x))
    customers = customerDataRDD.map(lambda item: {'custID': item['ID'], 'lat' : item['pickup_latitude'],\
                'long' :item['pickup_longitude'] , 'dropoff_lat': item['dropoff_latitude'], 'dropoff_long': item['dropoff_longitude']})
    customers.pprint()
    customers.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: getClosestDriver(x)))    

    # Start spark job
    ssc.start()
    ssc.awaitTermination()

