import json
import logging
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.typeinfo import Types
from openai import OpenAI
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

open_ai_key = os.getenv("OPENAI_API_KEY")

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
    
def extract_themes(review_text):
  try:
      client = OpenAI(api_key=open_ai_key)
      chat_completion = client.chat.completions.create(
          model="gpt-4",
          messages=[
              {"role": "user", "content": f"""Identify key themes in this product review: {review_text}
               
               Return a JSON array of themes"""}
          ]
      )

      content = chat_completion.choices[0].message.content
      
      logging.info(f"OpenAI API call successful: {content}")
      
      return json.loads(chat_completion.choices[0].message.content)
  except Exception as e:
      print(f"Error in extract_themes: {e}")
      return []
    
def process_reviews(event_str):
    try:
        review = clean_and_parse(event_str)
        review_text = review.get('review_text', '')
        review['themes'] = extract_themes(review_text)
        return json.dumps(review)
    except Exception:
        pass
    return None

if __name__ == '__main__':
  env = StreamExecutionEnvironment.get_execution_environment()
  env.set_parallelism(1)
  env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
  
  confluent_config = load_properties('client.properties')

  # Define Kafka Source
  source = KafkaSource.builder() \
      .set_bootstrap_servers(confluent_config.get("bootstrap.servers")) \
      .set_topics("product_reviews") \
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
            .set_topic("product_reviews_with_themes")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
      ) \
      .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
      .set_property("security.protocol", confluent_config.get("security.protocol")) \
      .set_property("sasl.mechanism", confluent_config.get("sasl.mechanism")) \
      .set_property("sasl.jaas.config", confluent_config.get("sasl.jaas.config")) \
      .build()
  
  # Extract themes from reviews
  themes_stream = ds.map(process_reviews, output_type=Types.STRING()) \
                        .filter(lambda x: x is not None)
                        
  themes_stream.sink_to(sink)

  # Define Kafka Source
  themes_source = KafkaSource.builder() \
      .set_bootstrap_servers(confluent_config.get("bootstrap.servers")) \
      .set_topics("product_reviews_with_themes") \
      .set_properties(confluent_config) \
      .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
      .set_value_only_deserializer(SimpleStringSchema()) \
      .build()
                        
  ds_themes = env.from_source(themes_source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Themes Source")

  # Process reviews and count themes
  theme_counts = ds_themes.map(clean_and_parse, output_type=Types.MAP(Types.STRING(), Types.OBJECT_ARRAY(Types.BYTE()))) \
        .filter(lambda x: x is not None and "themes" in x) \
        .flat_map(lambda review: [(theme.lower(), 1) for theme in review["themes"]], output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .sum(1)

  # Print results
  theme_counts.print()

  env.execute()