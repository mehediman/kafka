package com.samsung.dhl.producers;

import java.util.Properties;

abstract public class BasicProducer {
    String name;

    public abstract Properties getKafkaConfiguration();
    public abstract void startToConsume();
}
