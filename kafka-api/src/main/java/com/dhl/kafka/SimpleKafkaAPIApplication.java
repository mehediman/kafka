package com.dhl.kafka;

import com.dhl.kafka.consumers.SimpleKafkaConsumer;
import com.dhl.kafka.producers.SimpleKafkaProducer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

    @Value("${zookeeper.host}")
    String zookeeperHost;

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
            /*
             * Defining producer properties.
             */
            Properties producerProperties = new Properties();
            producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
            producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            producerProperties.put("ssl.truststore.location","/tmp/kafka.client.truststore.jks");
            producerProperties.put("acks", "all");
            producerProperties.put("retries", 0);
            producerProperties.put("batch.size", 16384);
            producerProperties.put("linger.ms", 1);
            producerProperties.put("buffer.memory", 33554432);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /*
        Creating a Kafka Producer object with the configuration above.
         */
            SimpleKafkaProducer kafkaProducer = new SimpleKafkaProducer(topicName, producerProperties);
            kafkaProducer.sendMessages();
        }
        else {
            /*
        Now that we've produced some test messages, let's see how to consume them using a Kafka consumer object.
         */

            /*
             * Defining Kafka consumer properties.
             */
            Properties consumerProperties = new Properties();
            consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
            consumerProperties.put("group.id", zookeeperGroupId);
            consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            consumerProperties.put("ssl.truststore.location","/tmp/kafka.client.truststore.jks");
            consumerProperties.put("zookeeper.session.timeout.ms", "6000");
            consumerProperties.put("zookeeper.sync.time.ms","2000");
            consumerProperties.put("auto.commit.enable", "true");
            consumerProperties.put("auto.commit.interval.ms", "1000");
            consumerProperties.put("consumer.timeout.ms", "-1");
            consumerProperties.put("max.poll.records", "1");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            /*
             * Creating a thread to listen to the kafka topic
             */
            Thread kafkaConsumerThread = new Thread(() -> {
                logger.info("Starting Kafka consumer thread.");

                SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(
                        topicName,
                        consumerProperties
                );

                simpleKafkaConsumer.runSingleWorker();
            });

            /*
             * Starting the first thread.
             */
            kafkaConsumerThread.start();
        }
    }
}
