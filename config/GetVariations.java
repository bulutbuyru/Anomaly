package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class GetVariations implements
        ValueJoiner<String, ArrayList<Integer>, ArrayList<Double>>{
    public ArrayList<Double> apply(String avg, ArrayList<Integer> values){
        Double deviation;
        Integer value;
        ArrayList<Double> variations = new ArrayList<>();
        for (int i =0; i < values.size(); i++) {
            value = values.get(i);
            deviation = (Math.pow(value - Float.parseFloat(avg), 2));
            variations.add(deviation);
        }

        return variations;
    }
}
