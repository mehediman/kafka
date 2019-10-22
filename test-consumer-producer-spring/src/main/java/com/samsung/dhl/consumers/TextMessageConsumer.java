package com.samsung.dhl.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.*;

public class TextMessageConsumer extends BasicConsumer {

    @Value("${consumer.id}")
    private String CONSUMER_ID;

    @Value("${consumer.group.id}")
    private String CONSUMER_GROUP_ID;

    @Value("${consumer.enable.auto.commit}")
    private boolean ENABLE_AUTO_COMMIT;

    @Value("${consumer.max.poll.records}")
    private int MAX_POLL_RECORDS;

    @Value("${consumer.auto.offset.reset}")
    private String AUTO_OFFSET_RESET;

    @Value("${consumer.key.deserializer}")
    private String KEY_DESERIALIZER;

    @Value("${consumer.value.deserializer}")
    private String VALUE_DESERIALIZER;

    private static final Logger logger = Logger.getLogger(TextMessageConsumer.class);

    private KafkaConsumer<String, String> kafkaConsumer;

    public TextMessageConsumer(List<String> consumerTopic) {
        setKafkaConsumerConfiguration();
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(consumerTopic);
    }

    @Override
    public void setKafkaConsumerConfiguration() {
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
    }

    @Override
    public void startToConsume() {
        //logger.info("RECORD OFFSET RESET TO 0");
        //kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        while(true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));

            for (ConsumerRecord<String, String> record : records) {

                String message = record.value();
                logger.info("Received text message: " + message);
                Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
                commitMessage.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                kafkaConsumer.commitSync(commitMessage);
                logger.info("Offset committed to Kafka.");
                logger.info("Record offset: " + record.offset());
            }
        }
    }
}
