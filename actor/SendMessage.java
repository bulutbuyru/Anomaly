package org.ergemp.fv.kproxy.actor;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SendMessage {
    public static Boolean send (Producer gProducer, String gTopic, String gKey, String gValue){
        Boolean retVal = true;

        String topic = gTopic;
        String value = gValue;
        String key = gKey;

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            gProducer.send(record);
            gProducer.flush();
            gProducer.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
            retVal = false;
        }
        finally {
            return retVal;
        }
    }

    public static Boolean send (Producer gProducer, String gTopic, String gValue){
        Boolean retVal = true;

        String topic = gTopic;
        String value = gValue;

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

            gProducer.send(record);
            gProducer.flush();
            gProducer.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
            retVal = false;
        }
        finally {
            return retVal;
        }
    }

}
