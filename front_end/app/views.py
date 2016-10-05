#### Views Script: Front end script that runs Kafka producer ####

from app import app
import redis 
from flask import jsonify
import ast
from custom_packages.elasticWrapper import ElasticWrapper
import json
from flask import render_template
from kafka import KafkaProducer


ew_cab_assign = ElasticWrapper('cab_assign')
ew_cab = ElasticWrapper('cab')
server_ips = ['172.31.2.14:9092', '172.31.2.6:9092', '172.31.2.4:9092', '172.31.2.5:9092']
producer = KafkaProducer(bootstrap_servers=server_ips)

def convert2Json(result):
    jsonResult = json.dumps(result)
    records = []
    ids = []
    for k, v in result.items():
        if str(k) == 'hits':
            for item in v['hits']:
                records.append(item['_source'])
                ids.append(item['_id'])
    return (records, ids)

@app.route('/')

@app.route('/bookmycab')
def bookmycab():
  user = { 'nickname': 'Miguel' } # fake user
  return render_template("bookCab.html", title = 'Home', user = user)

# Start page shown to user
@app.route('/bookmycab/<lat>/<lon>')
def getDriver(lat, lon):
    cab_query = {'sort': [{'_geo_distance': {'cab_location': {'lat': lat, 'lon': lon}, 'order': 'asc','unit':'m'}}],  "query": { "match_all": {} } } 
    result = ew_cab.search_document(cab_query)
    (finalResult, ids) = convert2Json(result)
    for item in finalResult:
        print item
    return jsonify(finalResult)
    
# This page is laoded when user hits 'Book now' button
@app.route('/booknow/<lat>/<lon>')
def bookCabNow(lat, lon):
   # call kafka producer from here with fake simulated user 
    custRequest = 'Umesh,2,2016-01-01 00:00:00,2016-01-01 00:00:00,5,4.90,'+str(lon)+','+str(lat)+', 1,N,-73.944473266601563,40.716678619384766,1,18,0.5,0.5,0,0,0.3,19.3'
    try:
        producer.send('customer-request', custRequest)
    except:
        e = sys.exc_info()[0]
        print e
    print 'Sent Message: '+ custRequest

    # For now, just make a blocking call reading database
    # TODO: Replace this code to web-socket using pub-sub model
    recordDB = redis.StrictRedis(host='172.31.2.14',  port=6379, db=1, password='abrakadabra')
    while True: 
        bookedCab = recordDB.get('Umesh')
        if str(bookedCab) != 'None':
            break
    print 'Booked Cab: '+ str(bookedCab)
    return jsonify({"success" : "ok", "cust": "Umesh", "cab": str(bookedCab)})

#@app.route('/latest-results')
"""
def get_latest():
    #print "In get Response"
    response_list = []
    jsonresponse = []
    dist_query = { 
      "query": {
        "filtered": {
          "filter": {
            "geo_distance": {
              "distance": "400",
              "cust_location": {
                "lat": 40.72,
                "lon": -73.99
              }   
            }   
          }   
        }   
      }   
    } 
    result = ew.search_document(dist_query)  
    recent_rec_query = { "query": { "match_all": {}},"size": 5,"sort": [{"_timestamp": {"order": "desc"}}]}
    result = ew_cab_assign.search_document(recent_rec_query)
    print result
    for x in response_list:
        jsonEntry = ast.literal_eval(x) 
        jsonresponse.append(jsonEntry)
    jsonEntry = json.dumps(result)

    records = []
    for k, v in result.items(): 
        #print item['cab_location'] + ", " + item['cust_location']
        if str(k) == 'hits':
            for item in v['hits']:
                records.append(item['_source'])
    return jsonify(records) 
"""
