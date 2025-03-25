import json
import time
import os
import random
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import Schema

users = [
    {"username": "Alice Johnson", "password": "444444"},
    {"username": "Bob Smith", "password": "123456"},
    {"username": "Charlie Brown", "password": "abcdfge"},
    {"username": "David White", "password": "admin"},
]

def generate_data(start_date="2024-01-01", end_date="2024-01-02"):
    user = random.choice(users)
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    random_time = start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))

    return {
        "bbc_led": str(random.randint(0, 1)),
        "bbc_fan": str(random.randint(0, 1)),
        "bbc_servo": str(random.randint(0, 1)),
        "bbc_control_fan": str(random.randint(0, 100)),
        "bbc_control_servo": str(random.randint(0, 100)),
        "bbc_name": str(user["username"]),
        "bbc_password": str(user["password"]),
        "timestamp": random_time.strftime("%Y-%m-%d %H:%M:%S")
    }

def load_avro_schema(file_path):
    if not os.path.exists(file_path):
        print(f"Lỗi: Không tìm thấy file schema {file_path}")
        exit(1)

    try:
        with open(file_path, 'r') as file:
            schema = json.load(file)
            if not schema:
                raise ValueError(f"Schema file {file_path} rỗng!")
            return json.dumps(schema)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Lỗi khi load schema {file_path}: {e}")
        exit(1)

def delivery_report(err, msg):
    if err:
        print(f"Lỗi khi gửi message: {err}")
    else:
        print(f"Message gửi thành công đến {msg.topic()} [{msg.partition()}] tại offset {msg.offset()}")

def register_schema_if_needed(schema_registry_client, subject_name, schema_str):
    try:
        subjects = schema_registry_client.get_subjects()
        if subject_name in subjects:
            print(f"Schema '{subject_name}' đã tồn tại.")
            return schema_registry_client.get_latest_version(subject_name).version

        schema = Schema(schema_str, "AVRO")
        schema_id = schema_registry_client.register_schema(subject_name, schema)
        print(f"Schema '{subject_name}' đã đăng ký với ID: {schema_id}")
        return schema_id
    except Exception as e:
        print(f"Lỗi khi đăng ký schema {subject_name}: {e}")
        return None

def main():
    topics = ['YoloHome']
    schema_registry_url = 'http://localhost:8082'
    avro_schemas_path = "../Avro_schema/schema.avsc"

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    schema_str = load_avro_schema(avro_schemas_path)

    register_schema_if_needed(schema_registry_client, "YoloSchema", schema_str)

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 100,
        "batch.size": 16384,
        "key.serializer": StringSerializer(),
        "value.serializer": None
    }

    producer = SerializingProducer(producer_config)
    cur_time = datetime.now()

    while (datetime.now() - cur_time).seconds < 10000:
        topic = random.choice(topics)
        data = generate_data()
        avro_data = avro_serializer(data, SerializationContext(topic, MessageField.VALUE))
        try:
            print(f"Producing Avro: {data}")
            producer.produce(topic=topic, value=avro_data, on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(5)
        except BufferError:
            print(f"Hàng đợi Producer đầy ({len(producer)} messages): Đang thử lại...")
            time.sleep(1)
        except Exception as e:
            print(f"Lỗi: {e}")
            time.sleep(10)

    producer.flush()

if __name__ == "__main__":
    main()
