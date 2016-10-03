## Producer Script for kafka: Reads from CSv and dumps into Kafka topics ##

from kafka import KafkaConsumer, KafkaProducer
import json
import yaml
import random
import threading, logging, time

server_ips = ['172.31.2.14:9092', '172.31.2.6:9092', '172.31.2.4:9092', '172.31.2.5:9092']

def main(): 
    producer = KafkaProducer(bootstrap_servers=server_ips)
    jsonFile = open("/home/ubuntu/bookMyCab/streaming_data/testData.csv", 'r')
    count, countOneLoop, cabID, custID = 0, 0, 0, 0
    cabIDMax = 10000
    custIDMax = 100000
    for line in jsonFile: 
        if count == 0: 
            count = count + 1
            continue
        rand_val = random.randint(0,1)
        #rand_val = 0
        if rand_val == 1:
            line = str(custID%custIDMax) + ',' + line
            producer.send('customer-request', line)
            print ("Customer request: "+line)
            custID = custID +1
        else:
            line = str(cabID%cabIDMax) + ',' + line
            producer.send('driver-request', line)
            print ("Driver request: "+line)
            cabID = cabID + 1
        count = count + 1 
        countOneLoop = countOneLoop + 1
        print ("sent json message"+line)
        if (countOneLoop > 500): 
            countOneLoop = 0
            time.sleep(1)
        
        #if count > 1000: 
         #   break

if __name__ == "__main__":
    main()
