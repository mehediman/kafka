package com.dhl.kafka;

import com.dhl.kafka.consumers.SimpleKafkaConsumer;
import com.dhl.kafka.producers.SimpleKafkaProducer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class SimpleKafkaAPIApplication implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topicName;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    private static final String CONSUMER = "c";
    private static final String PRODUCER = "p";

    private static final Logger logger = Logger.getLogger(SimpleKafkaAPIApplication.class);

    public static void main( String[] args ) {
        SpringApplication.run(SimpleKafkaAPIApplication.class, args);
    }

    @Override
    public void run(String[] args) {

        String process = (args.length > 1 && args[1].equalsIgnoreCase(CONSUMER)) ? CONSUMER : PRODUCER;

        if (process.equalsIgnoreCase(PRODUCER)) {

            Properties producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
            producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            producerProperties.put("ssl.truststore.location","/tmp/kafka.client.truststore.jks");
            producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
            producerProperties.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            SimpleKafkaProducer kafkaProducer = new SimpleKafkaProducer(topicName, producerProperties);
            kafkaProducer.sendMessages();
        }
        else {

            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, zookeeperGroupId);
            consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            consumerProperties.put("ssl.truststore.location","/tmp/kafka.client.truststore.jks");
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
            //consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


            Thread kafkaConsumerThread = new Thread(() -> {
                logger.info("Starting Kafka consumer thread.");

                SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(
                        topicName,
                        consumerProperties
                );

                simpleKafkaConsumer.runSingleWorker();
            });

            kafkaConsumerThread.start();
        }
    }
}
