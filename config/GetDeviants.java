package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;

public class GetDeviants implements
        ValueJoiner<String, ArrayList<Integer>, ArrayList<Integer>>  {

    public ArrayList<Integer> apply(String avgAndDeviation, ArrayList<Integer> values) {
        ArrayList<Integer> deviants = new ArrayList();
        String[] avgDevSplit = avgAndDeviation.split(" ");

        Double avg, dev;
        avg = Double.parseDouble(avgDevSplit[0]);
        dev = Double.parseDouble(avgDevSplit[1]);

        Integer value;
        for (int i = 0; i < values.size(); i++) {
            value = values.get(i);
            if (Math.abs(value - avg) > 2 * dev) deviants.add(value);
        }

        return deviants;
    }
}