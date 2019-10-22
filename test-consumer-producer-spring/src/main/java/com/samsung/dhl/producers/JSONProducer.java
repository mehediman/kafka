package com.samsung.dhl.producers;


import com.samsung.dhl.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.sql.Timestamp;

public class JSONProducer extends BasicProducer{

    private static final Logger logger = Logger.getLogger(JSONProducer.class);
    private KafkaProducer<String, String> kafkaProducer;
    private String KAFKA_TOPIC;

    public JSONProducer(String messageTopic) {
        setKafkaProducerConfiguration();
        this.kafkaProducer = new KafkaProducer<String, String>(properties);
        this.KAFKA_TOPIC = messageTopic;
    }

    @Override
    public void setKafkaProducerConfiguration() {
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfiguration.PRODUCER_ID);
        properties.put(ProducerConfig.ACKS_CONFIG, KafkaConfiguration.ACKS);
        properties.put(ProducerConfig.RETRIES_CONFIG, KafkaConfiguration.RETRIES);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConfiguration.LINGER_MS);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, KafkaConfiguration.BUFFER_MEMORY);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfiguration.KEY_SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfiguration.VALUE_SERIALIZER);
}

    @Override
    public void startToProduce() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        // produce 10 JSON messages
        for (int index = 0; index < 10; index++) {

            JSONObject jsonObject = new JSONObject();
            JSONObject nestedJsonObject = new JSONObject();

            try {
                jsonObject.put("index", index);
                jsonObject.put("message", "This is a no. " + index + " message at timestamp: " + timestamp.getTime());
                nestedJsonObject.put("nestedObjectMessage", "This is a nested JSON object with index: " + index);
                jsonObject.put("nestedJsonObject", nestedJsonObject);
                sendKafkaMessage(jsonObject.toString());

            } catch (JSONException e) {
                logger.error(e.getMessage());
            }
        }
        this.kafkaProducer.flush();
    }

    private void sendKafkaMessage(String payload)
    {
        logger.info("Sending Kafka message: " + payload);
        try {
            this.kafkaProducer.send(new ProducerRecord<>(this.KAFKA_TOPIC, payload));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
