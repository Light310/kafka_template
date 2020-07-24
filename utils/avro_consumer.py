from confluent_kafka import Consumer, TopicPartition
import io

from confluent_kafka import Consumer, KafkaError
from avro.io import DatumReader, BinaryDecoder
import avro.schema

from utils.load_avro_schema_from_registry import load_avro_schema_from_registry
from utils.parse_config import parse_kafka_config


class MyAvroConsumer():
    
    def __init__(self, schema_name, topic):
        kafka_cfg = parse_kafka_config()

        key_schema, value_schema = load_avro_schema_from_registry(schema_name, kafka_cfg['schema-registry-url'])
        self.schema = avro.schema.Parse(str(value_schema))      

        #size = 1000000
        self.topic = topic
        self.consumer = Consumer(
            {
                'bootstrap.servers': kafka_cfg['bootstrap-servers'],
                'group.id': 'mygroup',
                'auto.offset.reset': 'earliest',
            }
        )  

    @staticmethod
    def decode(schema, msg_value):
        reader = DatumReader(schema)
        message_bytes = io.BytesIO(msg_value)
        message_bytes.seek(5)
        decoder = BinaryDecoder(message_bytes)
        event_dict = reader.read(decoder)
        return event_dict

    def consume_session_window(self, timeout=1, session_max=5):
        session = 0
        while True:
            message = self.consumer.poll(timeout)
            if message is None:
                session += 1
                if session > session_max:
                    break
                continue
            if message.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            yield message
        self.consumer.close() 
                
    def consume(self, timeout):
        while True:
            message = self.consumer.poll(timeout)
            if message is None:
                continue
            if message.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            yield message
        self.consumer.close()

    # provide a function that accepts one dict parameter
    def confluent_consumer(self, func):
        self.consumer.subscribe([self.topic])
        for msg in self.consume(1.0):
            #print(msg.timestamp(), msg.topic(), self.decode(self.schema, msg.value()))
            func(self.decode(self.schema, msg.value()))

    def confluent_consumer_partition(self):
        self.consumer.assign([TopicPartition(self.topic, 0)])
        for msg in self.consume(1.0):
            print(msg)

"""
import json
import datetime

def process_msg(msg):
    print(msg['field1'])
    date = datetime.datetime.fromtimestamp(msg['field_dttm2'])
    print(date)
    print(msg['params'])
    print(msg['field_bool'])

AC = MyAvroConsumer('test_complex_schema_4', 'create-user-request-2')
AC.confluent_consumer(process_msg)
"""