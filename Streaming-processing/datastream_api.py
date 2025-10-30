import json
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
    KafkaSource)

## Thoát ra Dir lớn nhất đã rồi hãy chạy !!!
JARS_PATH = f"{os.getcwd()}/Streaming-processing/kafka-connect/jars/"  ## Để kết nối tới Jars của Kafka

print ("----Đọc folder JARS thành công !-----")

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-3.0.2-1.18.jar",
        f"file://{JARS_PATH}/kafka-clients-3.6.1.jar",
        f"file://{JARS_PATH}/flink-shaded-guava-31.1-jre-17.0.jar",
        f"file://{JARS_PATH}/flink-connector-base-1.18.1.jar",
    )

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("device_1")
        .set_group_id("device-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("NYC")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Create the data processing pipeline
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").sink_to(sink=sink)

    # Execute the job
    env.execute("flink_datastream_demo")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
