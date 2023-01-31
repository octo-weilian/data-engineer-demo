from confluent_kafka import Consumer, OFFSET_BEGINNING
from typing import Iterable
from .elastic import Elastic
import time
from datetime import datetime

class KafkaConsumer:
    host:str = "localhost:9092"
    group_id:str = 'weather_group'
    offset:str = 'earliest'
    
    def __init__(self):
        self.consumer = Consumer({'bootstrap.servers':self.host,'group.id':self.group_id,'auto.offset.reset':self.offset})

    def _read_messages(self,topic:str):
        """
        Dequeue data from topic
        """
    
        self.consumer.subscribe([topic])
        
        while True:
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue

            data =  msg.value().decode('utf-8')
            return data
    
        
    def listen(self,topic:str):
        
        print(f"Listening to {self.host} on topic {topic}: ")
      
        es_index = 'es0'+ topic
        es_client = Elastic()
        es_client.create_index(es_index)
        
        while True:
            message = self._read_messages(topic)
            if not message:
                continue
            time.sleep(0.1)
            es_client.push_to_index(es_index,message)
        
    
