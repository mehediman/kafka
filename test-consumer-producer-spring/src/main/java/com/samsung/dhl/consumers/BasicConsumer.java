package com.samsung.dhl.consumers;

import java.util.Properties;

abstract public class BasicConsumer {

    public String name;

    public abstract Properties getKafkaConfiguration();
    public abstract void startToConsume();
}
