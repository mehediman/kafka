package com.samsung.dhl.consumers;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

public class BasicConsumer {

    @Value("${kafka.bootstrap.servers}")
    protected String BOOTSTRAP_SERVERS;

    @Value("${kafka.security.protocol}")
    protected String SECURITY_PROTOCOL;

    @Value("${kafka.ssl.truststore.location}")
    protected String TRUSTSTORE_LOCATION;

    protected String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    protected Properties properties;

    public BasicConsumer() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
        properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCATION);
    }

    public void setKafkaConsumerConfiguration(){};
    public void startToConsume(){};
}
