package com.samsung.dhl;

public class KafkaConfiguration {

    // KAFKA Configuration
    public static String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static String BOOTSTRAP_SERVERS;
    public static String SECURITY_PROTOCOL;
    public static String TRUSTSTORE_LOCATION;

    // KAFKA Producer
    public static String PRODUCER_ID;
    public static String ACKS;
    public static int RETRIES;
    public static int BATCH_SIZE;
    public static long LINGER_MS;
    public static long BUFFER_MEMORY;
    public static String KEY_SERIALIZER;
    public static String VALUE_SERIALIZER;

    // KAFKA Consumer
    public static String CONSUMER_ID;
    public static String CONSUMER_GROUP_ID;
    public static boolean ENABLE_AUTO_COMMIT;
    public static int MAX_POLL_RECORDS;
    public static String AUTO_OFFSET_RESET;
    public static String KEY_DESERIALIZER;
    public static String VALUE_DESERIALIZER;

}
