package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

public class WindowedStreamMapper implements
        KeyValueMapper<Windowed<String>, String, KeyValue<String, String>> {
     public KeyValue <String, String> apply(Windowed<String> window, String val) {
         return new KeyValue<>(window.key(), val);
     }

}
