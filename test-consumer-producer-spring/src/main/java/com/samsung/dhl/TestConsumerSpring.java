package com.samsung.dhl;

import com.samsung.dhl.consumers.TextMessageConsumer;
import com.samsung.dhl.producers.TextMessageProducer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;


@SpringBootApplication
public class TestConsumerSpring implements CommandLineRunner {
    @Value("${kafka.topic}")
    private String KAFKA_TOPIC;

    private static final Logger logger = Logger.getLogger(TestConsumerSpring.class);

    public static void main( String[] args ) {
        SpringApplication.run(TestConsumerSpring.class, args);
    }

    @Override
    public void run(String[] args) {

        // Kafka producer application
        logger.info("Start producer...");
        TextMessageProducer producer = new TextMessageProducer(KAFKA_TOPIC);
        producer.startToProduce();

        // Kafka Consumer application
        logger.info("Start consumer...");
        TextMessageConsumer consumer = new TextMessageConsumer(Collections.singletonList(KAFKA_TOPIC));
        consumer.startToConsume();
    }
}
