package com.samsung.dhl.producers;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

public class BasicProducer {
    @Value("${kafka.bootstrap.servers}")
    protected String BOOTSTRAP_SERVERS;

    @Value("${kafka.security.protocol}")
    protected String SECURITY_PROTOCOL;

    @Value("${kafka.ssl.truststore.location}")
    protected String TRUSTSTORE_LOCATION;

    protected String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    protected Properties properties;

    private static final Logger logger = Logger.getLogger(BasicProducer.class);

    public BasicProducer() {
        properties = new Properties();
        logger.info(BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
        properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCATION);
    }

    public void setKafkaProducerConfiguration(){};
    public void startToProduce(){};
}
