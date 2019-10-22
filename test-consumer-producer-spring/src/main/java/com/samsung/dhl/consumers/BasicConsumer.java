package com.samsung.dhl.consumers;

import com.samsung.dhl.KafkaConfiguration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public abstract class BasicConsumer {

    Properties properties;

    BasicConsumer() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BOOTSTRAP_SERVERS);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KafkaConfiguration.SECURITY_PROTOCOL);
        properties.put(KafkaConfiguration.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaConfiguration.TRUSTSTORE_LOCATION);
    }

    public abstract void setKafkaConsumerConfiguration();
    public abstract void startToConsume();
}
