from confluent_kafka.admin import AdminClient, NewTopic
import sys

class KafkaClient:
 
    def __init__(self,host):
        self.client = AdminClient({'bootstrap.servers':host})
    
    def add_topic(self,topic:str)-> None:
        list_of_topics = self.client.list_topics().topics
        
        if list_of_topics.get(topic) is None:
            self.client.create_topics([NewTopic(topic,1,1)])
            
        return True
                
        