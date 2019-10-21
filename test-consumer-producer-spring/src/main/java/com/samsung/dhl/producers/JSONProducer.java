package com.samsung.dhl.producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.sql.Timestamp;
import java.util.Properties;

public class JSONProducer {

    private static final Logger logger = Logger.getLogger(JSONProducer.class);

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public JSONProducer(String messageTopic, Properties producerProperties) {
        kafkaProducer = new KafkaProducer<String, String>(producerProperties);
        topic = messageTopic;
    }

    public void sendMessages() {
        sendTestMessagesToKafka(kafkaProducer);
    }

    private void sendTestMessagesToKafka(KafkaProducer<String, String> producer) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        for (int index = 0; index < 10; index++) {
            sendKafkaMessage("The index is now: " + index + " Timestamp: " + timestamp.getTime(), producer, topic);
        }

        for (int index = 0; index < 10; index++) {

            JSONObject jsonObject = new JSONObject();
            JSONObject nestedJsonObject = new JSONObject();

            try {
                jsonObject.put("index", index);
                jsonObject.put("message", "The index is now: " + index + " Timestamp: " + timestamp.getTime());
                nestedJsonObject.put("nestedObjectMessage", "This is a nested JSON object with index: " + index);
                jsonObject.put("nestedJsonObject", nestedJsonObject);
                sendKafkaMessage(jsonObject.toString(), producer, topic);

            } catch (JSONException e) {
                logger.error(e.getMessage());
            }
        }
        producer.flush();
    }

    private void sendKafkaMessage(String payload,
                                         KafkaProducer<String, String> producer,
                                         String topic)
    {
        logger.info("Sending Kafka message: " + payload);
        try {
            producer.send(new ProducerRecord<>(topic, payload));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
