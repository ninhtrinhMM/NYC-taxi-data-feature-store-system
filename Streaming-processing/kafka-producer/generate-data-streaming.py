import argparse
import json
from datetime import datetime
from time import sleep

import numpy as np
from bson import json_util
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)

args = parser.parse_args()

def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass

def create_streams(servers):
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers) # Broker Kafka
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    while True:
        data = {}
        # Tạo value cho từng key trong data
        # Ép về string để tránh lỗi khi gửi message, vì schema đang định nghĩa type là string

        data["hvfhs_license_num"] = str(np.random.choice(["HV0003","HV0005"]))
        data["dispatching_base_num"] = str(np.random.choice(["B03404","B03406","B03405"]))
        data["originating_base_num"] = str(np.random.choice(["B03404","B03406","B03405"]))

        data["request_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data["on_scene_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data["pickup_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data["dropoff_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        data["tolls"]= str(np.random.randint(0, 6))
        data["bcf"]= str(np.random.uniform(0, 5.45))
        data["sales_tax"]= str(np.random.uniform(0.7, 15.68))
        data["congestion_surcharge"]= str(np.random.uniform(0, 2.75))
        data["airport_fee"]= str(np.random.randint(0, 3))
        data["tips"]= str(np.random.uniform(0, 8.48))
        data["shared_request_flag"]= str(np.random.choice(["Y","N"]))
        data["shared_match_flag"]= str(np.random.choice(["Y","N"]))
        data["access_a_ride_flag"]= str(np.random.choice(["Y","N"]))
        data["wav_request_flag"]= str(np.random.choice(["Y","N"]))
        data["wav_match_flag"]= str(np.random.choice(["Y","N"]))
        data["cbd_congestion_fee"]= str(np.random.choice([0, 1.5, 3, 4.5]))
        
        # Đọc column từ schema
        schema_path = f"./avro_schemas/schema.avsc" 
        with open(schema_path, "r") as f:
            parsed_schema = json.loads(f.read()) # Chuyển file từ string thành dict

        for a in parsed_schema["fields"]:  # áp dụng hàm for cho values của key "field"
            if a["field"] not in ["hvfhs_license_num", "dispatching_base_num", "originating_base_num"
                                     ,"request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime"
                                     ,"tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "tips", "shared_request_flag", "shared_match_flag"
                                    ,"access_a_ride_flag", "wav_request_flag", "wav_match_flag", "cbd_congestion_fee"]:
                                  
                data[a["field"]] = str(np.random.uniform(1.141, 700))

        topic_name = 'device_1' # Tạo biến Topic name

        # Sử dụng hàm create Topic để tạo topic
        create_topic(admin, topic_name=topic_name)

        # Tạo record, record là toàn bộ message gửi đi
        # Trong record có 2 Key lớn, Key 1 là schema, là Values của key "fields" trong file schema.avsc
        # Key 2 là payload, là toàn bộ data

        record = {
            "schema": {"type": "struct", "name": "nyc.taxi-record","fields": parsed_schema["fields"]},
            "payload": data,
        }
        # record = data # Message without schema

        # Bắn record đến topic tên là device_0 của broker
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        print(record)
        sleep(2)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass

if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]   # mode= 'setup'
    servers = parsed_args["bootstrap_servers"] 
    # servers= 'localhost:9092' = 'broker:29092'

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    if mode == "setup":
        try:
            teardown_stream("device_1", [servers])
        except Exception as e:
            print("Topic device_1 does not exist. Skipping...!")

        create_streams([servers])