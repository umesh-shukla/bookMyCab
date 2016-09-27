from elasticsearch import Elasticsearch, helpers as eshelpers
import pdb
import os
from datetime import datetime
import time
from kafka import KafkaConsumer, KafkaProducer
import json
import yaml
import random
import threading, logging, time
#from utils.utils import get_id, pprint
#from utils.utils import get_id, pprint
#See: https://elasticsearch-py.readthedocs.io/en/master/api.html


class ElasticWrapper():
    def __init__(self, index="geodata", type="point"):
        #public_dns = os.environ['PUBLIC_DNS']
        self.index = index
        self.type = type
        self.es =  Elasticsearch([{'host': 'localhost', 'port': 9200}])
    
    def list_indices(self):
        return self.es.indices.get_aliases().keys()

    def geo_index_exists(self):
        "Returns True if `self.index` exist"
        return self.index in self.list_indices()

    def create_geo_index(self):
        "Create an index for my geodata"
        config = {
            "mappings": {
                self.type :   {
                    "dynamic":"strict",
                    "properties" : {
                        "cust_id"  : { "type" : "string" },
                        "cust_location" : { "type" : "geo_point" }, 
                        "cab_id"  : { "type" : "string" },
                        "cab_location" : { "type" : "geo_point" }
                    }
                }
            }
        }
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
    
    def create_document_multi(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : get_id(),
                "_source": doc
            }
    
        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)

        
    def create_document_multi_id(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        Creates a ascending index
        """
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
    
    def search_document(self, query):
        return self.es.search(index=self.index, doc_type=self.type, body=query)
    
    def get_all(self):
        query = {"query" : {"match_all" : {}}}
        return self.es.search(index=self.index, doc_type=self.type, body=query)
