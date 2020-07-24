from confluent_kafka.admin import AdminClient, NewTopic
from parse_config import parse_kafka_config
from argparse import ArgumentParser
import json

# example usage:
# create_topic.py --topic test4 --partitions 2 --config '{"max.message.bytes": 15048576}'
def create_topic(topic_name, partitions=1, replicas=1, topic_config={}):
    # config example : 
    # topic_config = {"max.message.bytes": 15048576}

    kafka_cfg = parse_kafka_config()

    admin_client = AdminClient({
        "bootstrap.servers": kafka_cfg['bootstrap-servers']
    })

    if topic_config != {}:
        topic_config = json.loads(topic_config)
        
    futures = admin_client.create_topics([NewTopic(topic_name, partitions, replicas, config=topic_config)])
    
    for k, future in futures.items():
        future.result()

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--topic", required=True, help="Topic name")
    arg_parser.add_argument("--partitions", required=False, type=int, default=1, help="Number of partitions. Default 1")
    arg_parser.add_argument("--replicas", required=False, type=int, default=1, help="Number of replicas. Default 1")
    arg_parser.add_argument("--config", required=False, default={}, help="Config dictionary. Default empty")
    args = arg_parser.parse_args()
    create_topic(args.topic, args.partitions, args.replicas, args.config)

