Template for Kafka with python code

Prerequisites:
- docker-compose (tested on version 1.26.2, build eefe0d31)
- Docker (tested on version 18.09.7, build 2d0083d)
- Python (tested on version 3.8.3, with lower versions there are some troubles with kafka-libraries. Solvable, but takes time)

Usage:
- Spin up compose:
docker-compose up -d

- Install requirements (in a virtual environment)

- Create a topic:
python utils/create_topic.py --topic test

- Post a schema to registry:
python utils/post_schema_to_registry.py --schema complex-schema

- Run a consumer example:
python consumer_example.py

- Run a producer example (in another terminal):
python producer_example.py
(watch messages in a consumer terminal or in the Control Center UI)

Ports:
- Broker: 9092
- Schema registry: 8081
- Control center: 9021