package org.ergemp.fv.kproxy.config;

import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.List;

public class ListifyInit implements Initializer<ArrayList<Integer>> {
    public ArrayList<Integer> apply() {
        ArrayList<Integer> L = new ArrayList<>();
        return L;
    }
}
