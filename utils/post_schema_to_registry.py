from argparse import ArgumentParser

from confluent_kafka.schema_registry.schema_registry_client import Schema, SchemaRegistryClient

from load_avro_schema import load_avro_schema_from_file_plain_text
from parse_config import parse_kafka_config


# posts a schema to registry according to its file in 'avro' folder
# example usage : post_schema_to_registry.py --schema complex-schema
def post_schema_to_registry(schema_name):
    kafka_cfg = parse_kafka_config()

    schema_str = load_avro_schema_from_file_plain_text(schema_name + '.avsc')
    avro_schema = Schema(schema_str, 'AVRO')

    sr = SchemaRegistryClient({'url':kafka_cfg['schema-registry-url']})    
    _schema_id = sr.register_schema(schema_name, avro_schema)


if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--schema", required=True, help="Schema name")
    args = arg_parser.parse_args()
    post_schema_to_registry(args.schema)

# http://host:8081/subjects/
# http://host:8081/subjects/complex-schema/versions/latest
# http://135.181.34.254:8081/subjects/
# http://135.181.34.254:8081/subjects/complex-schema/versions/latest