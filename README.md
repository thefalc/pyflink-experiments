# Example PyFlink Jobs for Online Predictions and Real-time Monitoring
This repository contains examples of using PyFlink (Apache Flink) and Apache Kafak for doing online predictions and real-time monitoring.

The examples levearage [Confluent Cloud's](https://www.confluent.io/) Kafka support. Each example runs as a Flink job using the [DataSteam API](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/overview/).

## How it works
As a user, you can enter a lead the `incoming_leads` Kafak topic. This starts the multi-agent process. In a real application, a form or CRM would populate the 
`incoming_leads` topic.

# What you'll need
In order to set up and run the application, you need the following:

* [Java 21](https://www.oracle.com/java/technologies/downloads/)
* [Flink v2.1](https://nightlies.apache.org/flink/flink-docs-master/)
* [Python 3.10](https://www.python.org/)
* A [Confluent Cloud](https://www.confluent.io/) account
* An [OpenAI](https://openai.com/) API key

## Getting set up

### Get the starter code
In a terminal, clone the sample code to your project's working directory with the following command:

```shell
git clone https://github.com/thefalc/pyflink-experiments.git
```

### Setting up Apache Flink

* Downloads and install [Flink v2.1](https://nightlies.apache.org/flink/flink-docs-master/)
* Update /flink/conf/conf.yaml setting `taskmanager.numberOfTaskSlots: 2`
* Start the Flink cluster `/flink/bin/start-cluster.sh`

### Configure the AI SDR application

Go to your root folder and create a `.env` file with your OpenAI API key.

```bash
OPENAI_API_KEY=REPLACE_ME
```

Next, following the [instructions](https://docs.confluent.io/cloud/current/client-apps/config-client.html) to create a new Python client. Once the client downloads, unzip it and find the `client.properties` file. Copy this into the root project directory.

### Setting up Confluent Cloud

The sample scripts use Confluent Cloud's Kafka support to source and sink data.

#### Create the topics for anomaly detection and thematic analysis
In your Confluent Cloud account.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `sensor_data`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **sensor_data** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "device_id": {
      "type": "string"
    },
    "temperature": {
      "type": "number"
    },
    "timestamp": {
      "format": "date-time",
      "type": "string"
    }
  },
  "type": "object"
}
```

* Save the schema

Next, we are going to create a topic to sink anamolies to.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `sensor_anomalies`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **sensor_anomalies** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "anomaly": {
      "type": "boolean"
    },
    "device_id": {
      "type": "string"
    },
    "temperature": {
      "type": "number"
    },
    "timestamp": {
      "format": "date-time",
      "type": "string"
    }
  },
  "type": "object"
}
```

Next, we are going to create a topic for storing product reviews.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `product_reviews`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **product_reviews** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "product_id": {
      "description": "The unique identifier of the product.",
      "type": "string"
    },
    "rating": {
      "description": "The rating given by the user (1-5).",
      "maximum": 5,
      "minimum": 1,
      "type": "integer"
    },
    "review_text": {
      "description": "The text of the review.",
      "type": "string"
    },
    "review_timestamp": {
      "description": "The timestamp when the review was written.",
      "format": "date-time",
      "type": "string"
    },
    "user_id": {
      "description": "The unique identifier of the user who wrote the review.",
      "type": "string"
    }
  },
  "required": [
    "product_id",
    "user_id",
    "rating",
    "review_timestamp"
  ],
  "title": "ProductReview",
  "type": "object"
}
```

* Save the schema

Finally, we are going to create a topic to sink product review themes to.

* Go to your Kafka cluster and click on **Topics** in the sidebar.
* Name the topic as `product_reviews_with_themes`.
* Set other configurations as needed, such as the number of partitions and replication factor, based on your requirements.
* Go to **Schema Registry**
* Click **Add Schema** and select **product_reviews_with_themes** as the subject
* Choose JSON Schema as the schema type
* Paste the schema from below into the editor

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "properties": {
    "product_id": {
      "description": "The unique identifier of the product.",
      "type": "string"
    },
    "rating": {
      "description": "The rating given by the user (1-5).",
      "maximum": 5,
      "minimum": 1,
      "type": "integer"
    },
    "review_text": {
      "description": "The text of the review.",
      "type": "string"
    },
    "review_timestamp": {
      "description": "The timestamp when the review was written.",
      "format": "date-time",
      "type": "string"
    },
    "themes": {
      "description": "An array of themes or topics extracted from the review.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "user_id": {
      "description": "The unique identifier of the user who wrote the review.",
      "type": "string"
    }
  },
  "required": [
    "product_id",
    "user_id",
    "rating",
    "review_timestamp"
  ],
  "title": "ProductReview",
  "type": "object"
}
```

* Save the schema

### Configure the JAR file dependencies

To run PyFlink, you need to include the Java dependencies for Kafka.

* Download [flink-sql-connector-kafka-3.0.1-1.18.jar](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka) and place the *.jar file in the /lib directory.
* Download [flink-clients-2.0.0.jar](https://mvnrepository.com/artifact/org.apache.flink/flink-clients) and place the *.jar file in the /lib directory.

### Run the anamoly detection example

1. In a terminal, navigate to your project directory. Run the app with the following command:
```shell
pip install -r requirements.txt
flink run -py simple_anomaly_detection.py \
  --jarfile /ABSOLUTE_PATH_TO_PROJECT/lib/flink-sql-connector-kafka-3.0.1-1.18.jar --jarfile /ABSOLUTE_PATH_TO_PROJECT/lib/flink-clients-2.0.0.jar
```
2. In Confluent Cloud, navigate to the `sensor_data` topic.
3. Click **Actions > Produce new message**
4. Paste the following into the **Value** field.
```json
{
  "device_id": "sensor-999",
  "timestamp": "2023-10-27T10:30:00Z",
  "temperature": 115
}
```
5. If everything goes well, you'll have a similar message in the `sensor_anomalies` topic.

Experiment with different messages that don't have temperature's above 100 and see what happens.

### Run the thematic analysis example

1. In a terminal, navigate to your project directory. Run the app with the following command:
```shell
pip install -r requirements.txt
flink run -py thematic_analysis.py \
  --jarfile /ABSOLUTE_PATH_TO_PROJECT/lib/flink-sql-connector-kafka-3.0.1-1.18.jar --jarfile /ABSOLUTE_PATH_TO_PROJECT/lib/flink-clients-2.0.0.jar
```
2. In Confluent Cloud, navigate to the `product_reviews` topic.
3. Click **Actions > Produce new message**
4. Paste the following into the **Value** field.
```json
{
  "product_id": "product-123",
  "user_id": "user-456",
  "rating": 4,
  "review_text": "This product was great! I really enjoyed the features and the ease of use. Would definitely recommend it.",
  "review_timestamp": "2025-03-31T10:50:00Z"
}
```
5. If everything goes well, you'll a similar message in the `product_reviews_with_themes` topic that includes identified themes.

Keep adding new messages and see what themes get identified. Check the Flink logs for updated theme counts or sink the counts to another Kafka topic.