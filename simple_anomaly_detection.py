import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.typeinfo import Types

def load_properties(filepath):
    props = {}
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            props[key.strip()] = value.strip()
    return props

def clean_and_parse(raw_str):
    try:
        json_start = raw_str.find('{')
        if json_start == -1:
            raise ValueError("No JSON object found in payload.")

        json_str = raw_str[json_start:]
        return json.loads(json_str)
    except Exception as e:
        print(f"Failed to parse message: {e}")
        return None
    
def detect_anomalies(event_str):
    try:
        event = clean_and_parse(event_str)
        temperature = float(event.get('temperature', 0))
        if temperature > 100:  # simple threshold for demo
            event['anomaly'] = True
            return json.dumps(event)
    except Exception:
        pass
    return None

if __name__ == '__main__':
  confluent_config = load_properties('client.properties')

  env = StreamExecutionEnvironment.get_execution_environment()
  env.set_parallelism(1)
  env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

  # Define Kafka Source
  source = KafkaSource.builder() \
      .set_bootstrap_servers(confluent_config.get("bootstrap.servers")) \
      .set_topics("sensor_data") \
      .set_properties(confluent_config) \
      .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
      .set_value_only_deserializer(SimpleStringSchema()) \
      .build()

  ds = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")

  # Define Kafka Sink
  sink = KafkaSink.builder() \
      .set_bootstrap_servers(confluent_config.get("bootstrap.servers")) \
      .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("sensor_anomalies")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
      ) \
      .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
      .set_property("security.protocol", confluent_config.get("security.protocol")) \
      .set_property("sasl.mechanism", confluent_config.get("sasl.mechanism")) \
      .set_property("sasl.jaas.config", confluent_config.get("sasl.jaas.config")) \
      .build()
  
  # Detect anomalies on all source temperatures
  anomalies_stream = ds.map(detect_anomalies, output_type=Types.STRING()) \
                         .filter(lambda x: x is not None)

  anomalies_stream.sink_to(sink)

  env.execute()