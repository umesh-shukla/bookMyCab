from app import app
import redis 
from flask import jsonify
import ast
from custom_packages.elasticWrapper import ElasticWrapper
import json

@app.route('/')
@app.route('/index')
def index():
  return "Hello, World!"

ew = ElasticWrapper()
ew_cab_assign = ElasticWrapper('cab_assign')

@app.route('/latest-results')
def get_latest():
    #print "In get Response"
    """
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
    """
    recent_rec_query = { "query": { "match_all": {}},"size": 5,"sort": [{"_timestamp": {"order": "desc"}}]}
    result = ew_cab_assign.search_document(recent_rec_query)
    print result
    """
    for x in response_list:
        jsonEntry = ast.literal_eval(x) 
        jsonresponse.append(jsonEntry)
    """
    jsonEntry = json.dumps(result)

    records = []
    for k, v in result.items(): 
        #print item['cab_location'] + ", " + item['cust_location']
        if str(k) == 'hits':
            for item in v['hits']:
                records.append(item['_source'])
    return jsonify(records) 
