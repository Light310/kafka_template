import json
import uuid
#import datetime

from confluent_kafka.avro import AvroProducer

#from utils.load_avro_schema_from_file import load_avro_schema_from_file
from utils.load_avro_schema_from_registry import load_avro_schema_from_registry
#from parse_command_line_args import parse_command_line_args
from utils.parse_config import parse_kafka_config

class MyAvroProducer():

    def __init__(self, schema_name, topic):
        kafka_cfg = parse_kafka_config()
        key_schema, value_schema = load_avro_schema_from_registry(schema_name, kafka_cfg['schema-registry-url'])

        producer_config = {
            "bootstrap.servers": kafka_cfg['bootstrap-servers'],  
            "schema.registry.url": kafka_cfg['schema-registry-url'] 
        }

        self.topic = topic
        self.producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    def send_record(self, record_value, record_key=None):
        """
        kafka_cfg = parse_kafka_config()

        key_schema, value_schema = load_avro_schema_from_registry(schema_name, kafka_cfg['schema-registry-url'])

        
        producer_config = {
            "bootstrap.servers": kafka_cfg['bootstrap-servers'],  
            "schema.registry.url": kafka_cfg['schema-registry-url'] 
        }

        producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
        """

        key = record_key if record_key else str(uuid.uuid4())
        value = json.loads(record_value)
        
        
        try:
            self.producer.produce(topic=self.topic, key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value} to topic - {self.topic}: {e}")
        else:
            print(f"Successfully producing record value - {value} to topic - {self.topic}")
        

        self.producer.flush()

"""
if __name__ == "__main__":
    topic = 'create-user-request-2'
    schema_name = 'test_complex_schema_4'
    now = datetime.datetime.now().strftime('%s')
    record_value = '{"field1": "cluster", "field2": "object", "field3": "process", "params": {"key1":"value1", "key2":"value2"}, "field_bool": true, "field_dttm2":' + now + '}'
    AP = MyAvroProducer(schema_name, topic)
    AP.send_record(record_value)
"""