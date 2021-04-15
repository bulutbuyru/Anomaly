package org.ergemp.fv.kproxy.config;

import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final String CLIENT_ID_CONFIG = "KafkaAsyncProducer";

    public static final String KEY_SERIALIZER_CLASS_CONFIG =  StringSerializer.class.getName();
    public static final String VALUE_SERIALIZER_CLASS_CONFIG =  StringSerializer.class.getName();
}
