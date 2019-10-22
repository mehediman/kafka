package com.samsung.dhl.consumers;

import com.samsung.dhl.KafkaConfiguration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.time.Duration;
import java.util.*;

public class JSONConsumer extends BasicConsumer{

    private static final Logger logger = Logger.getLogger(JSONConsumer.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    public JSONConsumer(List<String> consumerTopic) {
        setKafkaConsumerConfiguration();
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(consumerTopic);
    }

    @Override
    public void setKafkaConsumerConfiguration() {
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConfiguration.CONSUMER_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfiguration.CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaConfiguration.ENABLE_AUTO_COMMIT);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConfiguration.MAX_POLL_RECORDS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfiguration.AUTO_OFFSET_RESET);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConfiguration.KEY_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConfiguration.VALUE_DESERIALIZER);
    }

    @Override
    public void startToConsume() {
        //logger.info("RECORD OFFSET RESET TO 0");
        //kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        while(true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));

            for (ConsumerRecord<String, String> record : records) {

                String message = record.value();
                logger.info("Received message: " + message);

                try {
                    JSONObject receivedJsonObject = new JSONObject(message);
                    logger.info("Index of de-serialized JSON object: " + receivedJsonObject.getInt("index"));
                } catch (JSONException e) {
                    logger.error(e.getMessage());
                }

                Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
                commitMessage.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                kafkaConsumer.commitSync(commitMessage);
                logger.info("Offset committed to Kafka.");
                logger.info("Record offset: " + record.offset());
            }
        }
    }
}
