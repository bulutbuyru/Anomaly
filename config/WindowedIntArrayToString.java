package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;

public class WindowedIntArrayToString implements
        KeyValueMapper<Windowed<String>, ArrayList<Integer>, KeyValue<String, String>> {
    public KeyValue <String, String> apply(Windowed<String> window, ArrayList<Integer> deviants) {
        String deviantsString = "";
        Integer val;
        for (int i = 0; i < deviants.size(); i ++) {
            val = deviants.get(i);
            deviantsString += Integer.toString(val) + ", ";
        }
        return new KeyValue<>(window.key(), deviantsString);
    }

}
