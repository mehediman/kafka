package com.samsung.dhl;

import com.samsung.dhl.consumers.JSONConsumer;
import com.samsung.dhl.consumers.TextMessageConsumer;
import com.samsung.dhl.producers.JSONProducer;
import com.samsung.dhl.producers.TextMessageProducer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;


@SpringBootApplication
public class TestConsumerSpring implements CommandLineRunner {

    // Common KAFKA Properties
    @Value("${kafka.bootstrap.servers}")
    private String BOOTSTRAP_SERVERS;

    @Value("${kafka.security.protocol}")
    private String SECURITY_PROTOCOL;

    @Value("${kafka.ssl.truststore.location}")
    private String TRUSTSTORE_LOCATION;

    @Value("${kafka.topic}")
    private String KAFKA_TOPIC;

    // Producer Properties
    @Value("${producer.id}")
    private String PRODUCER_ID;

    @Value("${producer.acks}")
    private String ACKS;

    @Value("${producer.retries}")
    private int RETRIES;

    @Value("${producer.batch.size}")
    private int BATCH_SIZE;

    @Value("${producer.linger.ms}")
    private long LINGER_MS;

    @Value("${producer.buffer.memory}")
    private long BUFFER_MEMORY;

    @Value("${producer.key.serializer}")
    private String KEY_SERIALIZER;

    @Value("${producer.value.serializer}")
    private String VALUE_SERIALIZER;

    // Consumer Properties
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

    private static final Logger logger = Logger.getLogger(TestConsumerSpring.class);

    public static void main( String[] args ) {
        SpringApplication.run(TestConsumerSpring.class, args);
    }

    @Override
    public void run(String[] args) {

        setKafkaConfiguration();

        // Kafka Producer
        logger.info("Start producer...");
        TextMessageProducer producer = new TextMessageProducer(KAFKA_TOPIC);
        producer.startToProduce();

        // Kafka Consumer
        logger.info("Start consumer...");
        TextMessageConsumer consumer = new TextMessageConsumer(Collections.singletonList(KAFKA_TOPIC));
        consumer.startToConsume();
    }

    private void setKafkaConfiguration() {
        // KAFKA Common Properties
        KafkaConfiguration.BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS;
        KafkaConfiguration.SECURITY_PROTOCOL = SECURITY_PROTOCOL;
        KafkaConfiguration.TRUSTSTORE_LOCATION = TRUSTSTORE_LOCATION;

        // KAFKA Producer
        KafkaConfiguration.PRODUCER_ID = PRODUCER_ID;
        KafkaConfiguration.ACKS = ACKS;
        KafkaConfiguration.RETRIES = RETRIES;
        KafkaConfiguration.BATCH_SIZE = BATCH_SIZE;
        KafkaConfiguration.LINGER_MS = LINGER_MS;
        KafkaConfiguration.BUFFER_MEMORY = BUFFER_MEMORY;
        KafkaConfiguration.KEY_SERIALIZER = KEY_SERIALIZER;
        KafkaConfiguration.VALUE_SERIALIZER = VALUE_SERIALIZER;

        // KAFKA Consumer
        KafkaConfiguration.CONSUMER_ID = CONSUMER_ID;
        KafkaConfiguration.CONSUMER_GROUP_ID = CONSUMER_GROUP_ID;
        KafkaConfiguration.ENABLE_AUTO_COMMIT = ENABLE_AUTO_COMMIT;
        KafkaConfiguration.MAX_POLL_RECORDS = MAX_POLL_RECORDS;
        KafkaConfiguration.AUTO_OFFSET_RESET = AUTO_OFFSET_RESET;
        KafkaConfiguration.KEY_DESERIALIZER = KEY_DESERIALIZER;
        KafkaConfiguration.VALUE_DESERIALIZER = VALUE_DESERIALIZER;
    }
}
