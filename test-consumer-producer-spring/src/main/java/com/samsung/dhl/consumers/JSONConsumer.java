package com.samsung.dhl.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.time.Duration;
import java.util.*;

public class JSONConsumer {

    private static final Logger logger = Logger.getLogger(JSONConsumer.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    public JSONConsumer(String topic, Properties consumerProperties) {

        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
    }

    public void runSingleWorker() {
        logger.info("RECORD OFFSET RESET TO 0");
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        while(true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));

            for (ConsumerRecord<String, String> record : records) {

                String message = record.value();
                logger.info("Received message: " + message);

                try {
                    JSONObject receivedJsonObject = new JSONObject(message);
                    logger.info("Index of deserialized JSON object: " + receivedJsonObject.getInt("index"));
                } catch (JSONException e) {
                    logger.error(e.getMessage());
                }

                {
                    Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
                    commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    kafkaConsumer.commitSync(commitMessage);
                    logger.info("Offset committed to Kafka.");
                    logger.info("Record offset: " + record.offset());
                }
            }
        }
    }
}
