package com.dhl.kafka.producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;
import java.sql.Timestamp;

public class SimpleKafkaProducer {

    private static final Logger logger = Logger.getLogger(SimpleKafkaProducer.class);
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public SimpleKafkaProducer(String messageTopic, Properties producerProperties) {
        kafkaProducer = new KafkaProducer<String, String>(producerProperties);
        topic = messageTopic;
    }

    public void sendMessages() {
        sendTestMessagesToKafka(kafkaProducer);
    }

    /**
     * Function to send some test messages to Kafka.
     * We'll get the Kafka producer object as a parameter to this function.
     * We'll generate some test messages, both simple strings and JSON objects, in a couple of
     * loops inside the function. We'll send these test messages to the topic in Kafka.
     *
     * @param producer The Kafka producer we created in the run() method earlier.
     */
    private void sendTestMessagesToKafka(KafkaProducer<String, String> producer) {
        /*
        Creating a loop which iterates 10 times, from 0 to 9, and sending a
        simple message to Kafka.
         */
        for (int index = 0; index < 10; index++) {
            sendKafkaMessage("The index is now: " + index + " Timestamp: " + timestamp.getTime(), producer, topic);
        }

        /*
        Creating a loop which iterates 10 times, from 0 to 9, and creates an instance of JSONObject
        in each iteration. We'll use this simple JSON object to illustrate how we can send a JSON
        object as a message in Kafka.
         */
        for (int index = 0; index < 10; index++) {

            /*
            We'll create a JSON object which will have a bunch of fields, and another JSON object,
            which will be nested inside the first JSON object. This is just to demonstrate how
            complex objects could be serialized and sent to topics in Kafka.
             */
            JSONObject jsonObject = new JSONObject();
            JSONObject nestedJsonObject = new JSONObject();

            try {
                /*
                Adding some random data into the JSON object.
                 */
                jsonObject.put("index", index);
                jsonObject.put("message", "The index is now: " + index + " Timestamp: " + timestamp.getTime());

                /*
                We're adding a field in the nested JSON object.
                 */
                nestedJsonObject.put("nestedObjectMessage", "This is a nested JSON object with index: " + index);

                /*
                Adding the nexted JSON object to the main JSON object.
                 */
                jsonObject.put("nestedJsonObject", nestedJsonObject);

            } catch (JSONException e) {
                logger.error(e.getMessage());
            }

            /*
            We'll now serialize the JSON object we created above, and send it to the same topic in Kafka,
            using the same function we used earlier.
            You can use any JSON library for this, just make sure it serializes your objects properly.
            A popular alternative to the one I've used is Gson.
             */
            sendKafkaMessage(jsonObject.toString(), producer, topic);
            producer.flush();
        }
    }

    /**
     * Function to send a message to Kafka
     * @param payload The String message that we wish to send to the Kafka topic
     * @param producer The KafkaProducer object
     * @param topic The topic to which we want to send the message
     */
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
