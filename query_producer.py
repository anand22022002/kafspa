import os

# Run Kafka Console Consumer to view data in the topic
print("Displaying data published by Kafka Producer:")
os.system("./../bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wind-data --from-beginning")
