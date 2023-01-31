from demo import Elastic 
from demo import KafkaProducer,KafkaConsumer
import argparse

def main():
    """
    Run Elastik and Kafka
    """
    
    print('Running demo...')
    
    parser = argparse.ArgumentParser()
    parser.add_argument('method', type=str)
    parser.add_argument('topic', type=str)
    args =  parser.parse_args()
    
    if args.method == 'notify' and args.topic is not None:
        producer = KafkaProducer()
        producer.notify(args.topic)
        
    if args.method == 'listen' and args.topic is not None:
        consumer = KafkaConsumer()
        consumer.listen(args.topic)
        
if __name__ == "__main__":
    
    main()