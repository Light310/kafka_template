import datetime

from utils.avro_producer import MyAvroProducer


schema_name = 'complex-schema'
topic = 'test'

AP = MyAvroProducer(schema_name, topic)
for i in range(100):
    now = datetime.datetime.now().strftime('%s')
    record_value = '{"field1": "cluster", "field2": "object", "field3": "process", "field_amt":' + str(i) + ', "params": {"key1":"value1", "key2":"value2"}, "field_bool": true, "field_dttm2":' + now + '}'
    AP.send_record(record_value)