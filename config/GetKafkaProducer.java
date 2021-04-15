package org.ergemp.fv.kproxy.config;

import kafka.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class GetKafkaProducer {
    public static Producer get (String gClientId){
        Producer producer = null;
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS_CONFIG);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, gClientId);  //client.id
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfig.KEY_SERIALIZER_CLASS_CONFIG);  //key.serializer
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfig.VALUE_SERIALIZER_CLASS_CONFIG);  //value.serializer

            producer = new KafkaProducer<String, String>(props);
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            return producer;
        }
    }
}
