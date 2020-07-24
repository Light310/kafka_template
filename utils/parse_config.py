import yaml

def parse_kafka_config():
    with open("config/main-config.yml", 'r') as stream:
        try:
            cfg = yaml.safe_load(stream)        
            return cfg['kafka']
        except yaml.YAMLError as exc:
            print(exc)

