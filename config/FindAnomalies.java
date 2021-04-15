package org.ergemp.fv.kproxy.config;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.ergemp.fv.kproxy.model.ArrayListSerde;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.util.Properties;

public class FindAnomalies {

    //TODO: right now input is assumed to be a stream of integers.
    //this is obviously an unreasonable assumption.

    //TODO: possibly store less variables
    //TODO: use less int-float-double casting and string parsing - keep things in the same type
    //TODO: possibly keep <Windowed<String>> as keys, and use windowed string serde as default key serde.
    //this would especially be useful - and it makes more sense; as windowedStreamMapper is often used,
    //and does nothing but map <Windowed<String>> key streams to <String> key streams.
    static Topology createTopology() {
        String INPUT_TOPIC,SUM_TOPIC,AVG_TOPIC,COUNT_TOPIC,DEVIATION_TOPIC, DEVIANTS_TOPIC;
        StreamsBuilder builder = new StreamsBuilder();

        Topology topology;
        KStream        <String, String>   streamInputString, streamAdded, streamCount, streamAvg;
        KStream        <String, Integer>  streamInputInt;
        KStream        <Windowed<String>, String> streamAddedWindowed, streamCountWindowed,
                streamAvgWindowed, streamDeviationWindowed, avgDevWindowed;
        KStream        <Windowed<String>, Double> variationsWindowed, streamVariationsSumWindowed;
        KTable         <Windowed<String>, Integer> tableAddedWindowed;
        KTable         <Windowed<String>, String> tableAvgWindowed;
        KTable         <Windowed<String>, Long> tableCountWindowed;
        KGroupedStream <Windowed<String>, Double> variationsGrouped;
        KGroupedStream <String, Integer>  groupedInputInt;


        //useful stuff for later -- functions that are instances of classes that are called multiple times
        // and Serde stuff
        WindowedStreamMapper    windowedStreamMapper = new WindowedStreamMapper();
        Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        ArrayListSerde<Integer> intArraySerde = new ArrayListSerde<>(Serdes.Integer());
        Materialized<String, ArrayList<Integer>, WindowStore<Bytes, byte[]>>
            windowIntArrayMat = Materialized.with(Serdes.String(), intArraySerde);

        Materialized<Windowed<String>, Double, KeyValueStore<Bytes, byte[]>>
            materializedWindowDouble = Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Double());

        //topics
        INPUT_TOPIC    = "streams-input";
        SUM_TOPIC      = "streams-added";
        AVG_TOPIC      = "streams-avg";
        COUNT_TOPIC    = "streams-count";
        DEVIATION_TOPIC= "streams-deviation";
        DEVIANTS_TOPIC = "streams-deviants";

        /*
        logic to calculate sum of records
         string stream(input) ->  int stream(intermediate) ->
         ...int group(group by key for reduce) ->
         ...int table(reduced) -> string stream(why not)
        */
        streamInputString   = builder.stream(INPUT_TOPIC);
        streamInputInt      = streamInputString.mapValues((ValueMapper<String, Integer>) Integer::parseInt);
        groupedInputInt     = streamInputInt.groupByKey(Grouped.valueSerde(Serdes.Integer()));
        TimeWindowedKStream<String, Integer> streamInputWindowed =
                groupedInputInt.windowedBy(TimeWindows.of(Duration.ofSeconds(10)));

        tableAddedWindowed = streamInputWindowed.reduce(Integer::sum); //values here are still integers
        streamAddedWindowed = tableAddedWindowed.toStream().mapValues(Object::toString); //values here are string
        streamAdded = streamAddedWindowed.map(windowedStreamMapper); //no longer windowed

        //logic for counting how records in windows
        tableCountWindowed = streamInputWindowed.count();
        streamCountWindowed = tableCountWindowed.toStream().mapValues(Object::toString); //values here are string
        streamCount = streamCountWindowed.map(windowedStreamMapper); //no longer windowed

        //logic for averages
        streamAvgWindowed =
                streamAddedWindowed.join(tableCountWindowed,
                    (sum, count) -> Float.toString((float) (Integer.parseInt(sum)) / count));
        /*
        abi sonraki line cok salak. KStream.toTable diye bir method olmasi lazim, fakat yok.
        ben bu streami manuel olarak tablea ceviriyorum, cunku daha sonra joinlemem lazim
        stream to stream join windowed yapiliyor - bana unwindowed join lazim.
        cunku zaten bunlarin keyi windowed.
        */
        tableAvgWindowed = streamAvgWindowed.groupByKey(Grouped.keySerde(windowedSerde)).reduce((v1,v2)->v2);
        streamAvg = streamAvgWindowed.map(windowedStreamMapper);

        // LISTIFY INPUT
        // if streamInputWindowed has records 2,3,4 in window0, and 5,6 in window1,
        // then inputListTableWindowed has record [2,3,4] in window0, and [5,6] in window1.
        KTable <Windowed<String>, ArrayList<Integer>> inputListTableWindowed =
                streamInputWindowed.aggregate(new ListifyInit(), new ListifyAggregate(), windowIntArrayMat);

        //join avg with input list to find variations of all input from the average.
        //result is still a windowed stream, but with double array as key
        //this is because variations from the average are possibly not integers.
        KStream<Windowed<String>, ArrayList<Double>> windowedListVariations =
                streamAvgWindowed.join(inputListTableWindowed, new GetVariations());

        //flatten list of variations into variations - if stream has record [2,3,4],
        // then result has records 2,3,4
        variationsWindowed =
                windowedListVariations.flatMapValues((ValueMapper<List<Double>, Iterable<Double>>) doubles -> doubles);
        variationsGrouped = variationsWindowed.groupByKey(Grouped.valueSerde(Serdes.Double()));

        //logic for finding deviation
        streamVariationsSumWindowed = variationsGrouped.reduce(Double::sum, materializedWindowDouble).toStream();
        streamDeviationWindowed = streamVariationsSumWindowed.join(tableCountWindowed,
                (variationsSum, count) -> variationsSum/count).mapValues(v->Math.pow(v,0.5))
            .mapValues(Object::toString);

        // TODO: combine avg, dev in a class; write serializer for it instead using this.
        avgDevWindowed = streamDeviationWindowed.join(tableAvgWindowed, (dev, avg) -> avg + " " + dev);

        //deviants into windowed stream with int array as values
        KStream<Windowed<String>, ArrayList<Integer>> streamDeviantsWindowed =
            avgDevWindowed.join(inputListTableWindowed, new GetDeviants());

        //convert windowed array stream to string string stream
        KStream<String, String> streamDeviants = streamDeviantsWindowed.map(new WindowedIntArrayToString());

        streamAdded.to(SUM_TOPIC);
        streamCount.to(COUNT_TOPIC);
        streamAvg.to(AVG_TOPIC);
        avgDevWindowed.map(windowedStreamMapper).to(DEVIATION_TOPIC);

        streamDeviants.to(DEVIANTS_TOPIC);
        topology = builder.build();
        return topology;
    }


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-application2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
