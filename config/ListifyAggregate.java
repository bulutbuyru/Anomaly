package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.kstream.Aggregator;

import java.util.ArrayList;
import java.util.List;

public class ListifyAggregate implements
        Aggregator<String, Integer, ArrayList<Integer>> {
    public ArrayList<Integer> apply (String key, Integer value, ArrayList<Integer> agg) {
        agg.add(value);
        return agg;
    }

}
