from elasticsearch import Elasticsearch, helpers
from pathlib import Path
import sys
import json
from datetime import datetime

class Elastic:
    es_host:str = 'http://localhost:9200'
    
    def __init__(self) -> None:
        self.es = self._connect()
    
    def _connect(self) -> None:
        try:
            es = Elasticsearch(self.es_host)
            if (cluster_name:=es.info().get('cluster_name',None)):
                print(f'Connected to Elasticsearch cluster: {cluster_name}')
        except:
            print(f"Unable to connect. Check if Elasticsearch is running on {self.es_host}. Program exited.")
            sys.exit()
        else:
            return es
    
    def create_index(self,name:str) -> None:
      
        if not self.es.indices.exists(index=name):
            request_body = {"settings":{"number_of_shards":1,"number_of_replicas":0}}
            response = self.es.indices.create(index=name,body=request_body,ignore=400)
            print(f'Create index: {response}')
    
    def push_to_index(self,name,message):
        try:
            message = json.loads(message)
            message['timestamp'] = datetime.now()
            
            response = self.es.index(index=name,document=message)
            print(f'Push to index: {response}')
        except Exception as e:
            print(f"Error: {e}")