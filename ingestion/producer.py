## Producer Script for kafka: Reads from CSv and dumps into Kafka topics ##

from kafka import KafkaConsumer, KafkaProducer
import json
import yaml
import random
import threading, logging, time

def main(): 
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    jsonFile = open("/home/ubuntu/bookMyCab/streaming_data/testData.csv", 'r')
    count = 0 
    cabID = 0
    custID = 0
    cabIDMax = 10000
    custIDMax = 100000
    for line in jsonFile: 
        if count == 0: 
            count = count + 1
            continue
        #a_val = random.randint(1,100)
        #b_val = random.randint(1,10)
        #producer.send('customer-request', {"A": a_val, "B": b_val})
        rand_val = random.randint(0,1)
        if rand_val == 0:
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
        print ("sent json message"+line)
        time.sleep(1)
        if count > 100: 
            break

if __name__ == "__main__":
    main()
