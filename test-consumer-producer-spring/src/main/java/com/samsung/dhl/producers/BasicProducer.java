package com.samsung.dhl.producers;

import com.samsung.dhl.KafkaConfiguration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

public abstract class BasicProducer {
    Properties properties;

    private static final Logger logger = Logger.getLogger(BasicProducer.class);

    BasicProducer() {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BOOTSTRAP_SERVERS);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KafkaConfiguration.SECURITY_PROTOCOL);
        properties.put(KafkaConfiguration.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaConfiguration.TRUSTSTORE_LOCATION);
    }

    public abstract void setKafkaProducerConfiguration();
    public abstract void startToProduce();
}
