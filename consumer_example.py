import json
import datetime

from utils.avro_consumer import MyAvroConsumer


schema_name = 'complex-schema'
topic = 'test'

def process_msg(msg):
    date = datetime.datetime.fromtimestamp(msg['field_dttm2'])
    print('Processing : ' + str(date) + '. ' + str(msg))


AC = MyAvroConsumer(schema_name, topic)
AC.confluent_consumer(process_msg)