from confluent_kafka import avro
import requests

"""
schema_registry_url = 'http://localhost:8081'
schema_name = 'create-user-request-value'
print(load_avro_schema_from_registry(schema_name, schema_registry_url))
"""

def load_avro_schema_from_registry(schema_name, schema_registry_url):
    key_schema_string = """
    {"type": "string"}
    """

    response = requests.get(f'{schema_registry_url}/subjects/{schema_name}/versions/latest')

    key_schema = avro.loads(key_schema_string)
    value_schema = avro.loads(response.json().get("schema"))

    return key_schema, value_schema
