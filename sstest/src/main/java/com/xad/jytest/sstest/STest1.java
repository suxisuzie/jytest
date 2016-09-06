package com.xad.jytest.sstest;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jamesyu on 9/1/16.
 */
public class STest1 {
    /**
     * brokers
     * topics
     *
     * @param args
     */

    private static void process(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("jytest-ss");
        // 1 minute time batch
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1000));

        String brokers = args[0];
        String topics = args[1];

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._1 + "___" + tuple2._2();
            }
        });

        lines.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        process(args);
    }
}



