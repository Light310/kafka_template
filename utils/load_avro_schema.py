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


def load_avro_schema_from_file(schema_file):
    key_schema_string = """
    {"type": "string"}
    """

    key_schema = avro.loads(key_schema_string)
    value_schema = avro.load("./avro/" + schema_file)

    return key_schema, value_schema

"""
Don't know why, but schema_registry_client can't parse schema if it was loaded with avro.load.
For that case loading schema as plain text
"""
def load_avro_schema_from_file_plain_text(schema_file):

    with open("./avro/" + schema_file, 'r') as f:
        contents = ''.join(f.readlines())

    return contents
