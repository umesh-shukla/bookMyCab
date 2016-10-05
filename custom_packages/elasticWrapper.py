## Basic Wrapper for Elastic Search DB ##

from elasticsearch import Elasticsearch, helpers as eshelpers
import pdb
import os
from datetime import datetime
import time
import json
import yaml
import random
import threading, logging, time

class ElasticWrapper():
    def __init__(self, index="geodata", type="point"):
        self.index = index
        self.type = type
        self.es =  Elasticsearch([{'host': '172.31.2.14', 'port': 9200}])
    
    def list_indices(self):
        return self.es.indices.get_aliases().keys()

    def geo_index_exists(self):
        "Returns True if `self.index` exist"
        return self.index in self.list_indices()

    def create_geo_index(self, config):
        "Create an index for my geodata"
        return self.es.indices.create(index=self.index, body=config)

    def get_mapping(self):
        return self.es.indices.get_mapping(index=self.index, doc_type=self.type)

    def delete_index(self):
        "Deletes one or more indices"
        return self.es.indices.delete(index=[self.index])
    
    def create_document(self, doc):
        identifier = datetime.now() 
        print identifier
        return self.es.create(index=self.index, doc_type=self.type, body=doc, id=identifier)
    
    # Bulk create 
    def create_document_multi(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : datetime.now(),
                "_source": doc
            }
    
        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)
       
    """ 
    def create_document_multi_id(self, docs):
        #Bulk indexes multiple documents. 
        #docs is a list of document objects.
        #Creates a ascending index
        prefix = get_id()[:10]

        def add_meta_fields(doc, i):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : "{}-{}".format(prefix, str(i).zfill(6)),
                "_source": doc
            }
    
        docs = [add_meta_fields(doc, i) for i, doc in enumerate(docs)]
        return eshelpers.bulk(self.es, docs)
   """ 
    def search_document(self, query):
        return self.es.search(index=self.index, doc_type=self.type, body=query)
   
    # Bulk query  
    def search_document_multi(self, docs):
        """
        Bulk indexes multiple documents.
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            #return "{}\n" + json.dumps(doc) + "\n"
            return '{}\n{"query":{"filtered": {"filter": {"geo_distance": {"distance": "400", \
                    "location": {"lat": "'+str(doc["lat"])+'", "lon": "'+str(doc["lon"])+'"}}}}}}\n'

        print docs
        return self.es.msearch(index=self.index, search_type="query_and_fetch", body=docs)

    def get_all(self):
        query = {"query" : {"match_all" : {}}}
        return self.es.search(index=self.index, doc_type=self.type, body=query)

    def delete_doc_by_index(self, idx):
        return self.es.delete(index=self.index,doc_type=self.type, id=idx)

    # Query to get closest cab to the user
    def get_closest_items_bulk_query(self, docs):
        """ 
        Bulk indexes multiple documents.
        docs is a list of document objects.
        """
        if len(docs) == 0:
            return
        
        def add_meta_fields(doc):
            #return "{}\n" + json.dumps(doc) + "\n"
            return '{}\n{"sort": [{"_geo_distance": {"cab_location":{"lat":'+str(doc["lat"])+',"lon":'+str(doc["long"])+'}, "order": "asc","unit":"m"}}], "query": { "match_all": {} } } \n'

        docs = map(add_meta_fields, docs)
        #print docs
        return self.es.msearch(index=self.index, search_type="query_and_fetch", body=docs)
