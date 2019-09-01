# data_streaming
## Beginning the Project
This project requires creating topics, starting Zookeeper and Kafka server, and your Kafka bootstrap server. 
Use the commands below to start Zookeeper and Kafka server.
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

You can start the server using this Python command:

python producer_server.py

### Step 1
The first step is to build a simple Kafka server.
Complete the server in data_producer/producer_server.py.
To see if you correctly implemented the server, use this command to see your output:
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic udacity-streaming-project  --from-beginning
```

### Step 2
Apache Spark already has an integration with Kafka Brokers, hence we will not need a separate Kafka Consumer.
Implement features in streaming/data_stream.py.
Do a spark-submit using this command:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local ./streaming/data_stream.py 
```


