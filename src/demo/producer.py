import json
import time
import random
from confluent_kafka import Producer
import sys
from .client import KafkaClient

class KafkaProducer:
    host:str = "localhost:9092"
    
    def __init__(self) -> None:
        """
        Instantiate producer and fake data generator
        """
        self.producer = Producer({'bootstrap.servers': self.host})
        self.client = KafkaClient(self.host)
        
    def _event_callback(self,err:str,msg:str) -> None:
        """
        Callback message or errors
        """
        
        if err is not None:
            print('Error: {}'.format(err))
        else:
            message = msg.value()
            print(message.decode('utf-8'))
    
    def mock_data(self) -> str:
        data = {
            'temp':random.randint(6,15),
            'windkmh':random.randint(5,14),
            'lv':random.randint(50,80)
            }
        return json.dumps(data).encode('utf-8')

    def notify(self,topic:str) -> None:
        "Enqeue data to a Kafka topic "
        
        if self.client.add_topic(topic) is not None:
            print(f"Notifying to {self.host} on topic {topic}: ")
            
            while True:
                
                message = self.mock_data()
                
                self.producer.poll(1)
                self.producer.produce(topic=topic,value=message,callback=self._event_callback)
                self.producer.flush()
                
                time.sleep(1)
            
               
