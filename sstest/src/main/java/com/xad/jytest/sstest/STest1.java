package com.xad.jytest.sstest;

import com.google.protobuf.Message;
import com.xad.enigma.*;
import com.xad.enigma.EnigmaEventFramework.EnigmaEnvelope;
import com.xad.enigma.eventmodel.core.Topic;
import kafka.serializer.DefaultDecoder;
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

    public final static String EXT = ".avro";

    private static void process(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("jytest-ss");
        // 1 minute time batch

        //just do this current in the future change the properties ?

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));


        String brokers = args[0];
        String topics = args[1];

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        JavaPairInputDStream<byte[], EnigmaEnvelope> directStream = KafkaUtils.createDirectStream(
                jssc,
                byte[].class,
                EnigmaEnvelope.class,
                DefaultDecoder.class,
                EnigmaKafkaDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        /*JavaDStream<String> lines = directStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._1 + "___" + tuple2._2();
            }
        });

        lines.print();*/
        // final DataFileWriter<Object> writer = new DataFileWriter<>(new ProtobufDatumWriter<>());
        // Path path = getOutputPath();

        JavaDStream<Message> message = directStream.map(new Function<Tuple2<byte[], EnigmaEnvelope>, Message>() {
            @Override
            public Message call(Tuple2<byte[], EnigmaEnvelope> tuple2) {
                try {
                    EnigmaEnvelope envelope = tuple2._2();


                    Class<com.google.protobuf.Message> protoClass = Topic.fromName(envelope.getEventTopic()).protoClass;

                    String topic = envelope.getEventTopic();
                    byte[] data = envelope.getEventData().toByteArray();

                    switch (topic) {
                        case "enigma.event.AdDemandPartnerReportingRequest":
                            return AdDemandPartnerReportingRequestTopic.AdDemandPartnerReportingRequest.parseFrom(data);
                        case "enigma.event.AdRequest":
                            return AdRequestTopic.AdRequest.parseFrom(data);
                        case "enigma.event.AdDetails":
                            return AdDetailsTopic.AdDetails.parseFrom(data);
                        case "enigma.event.AdTracking":
                            return AdTrackingTopic.AdTracking.parseFrom(data);
                        case "enigma.event.AdUserProfile":
                            return AdUserProfileTopic.AdUserProfile.parseFrom(data);
                        case "enigma.event.UpdateUserProfile":
                            return AdUserProfileTopic.AdUserProfile.parseFrom(data);
                        case "enigma.event.DelUserProfile":
                            return DelUserProfileTopic.DelUserProfile.parseFrom(data);
                        case "enigma.event.SampleTracking":
                            return SampleTrackingTopic.SampleTracking.parseFrom(data);
                        case "enigma.event.HttpVendorStats":
                            return HttpVendorStatsTopic.HttpVendorStats.parseFrom(data);
                        case "enigma.event.SegmentBuilder":
                            return SegmentBuilderTopic.SegmentBuilder.parseFrom(data);
                        case "enigma.event.UserTracking":
                            return UserTrackingTopic.UserTracking.parseFrom(data);
                        case "enigma.event.BlockAttributes":
                            return BlockAttributesTopic.BlockAttributes.parseFrom(data);
                        case "enigma.event.AdDocument":
                            return AdDocumentTopic.AdDocument.parseFrom(data);
                        case "enigma.event.RTITracking":
                            return RTITrackingTopic.RTITracking.parseFrom(data);
                        case "enigma.event.rev.AtlanticMetaData":
                            return AtlanticMetaDataTopic.AtlanticMetaData.parseFrom(data);
                        case "enigma.event.VisitTracking":
                            return VisitTrackingTopic.VisitTracking.parseFrom(data);
                        case "enigma.event.AdExposure":
                            return VisitTrackingTopic.AdExposure.parseFrom(data);
                        case "enigma.event.AdExposureState":
                            return VisitTrackingTopic.AdExposure.parseFrom(data);
                        case "enigma.event.SvlControlGroup":
                            return ControlGroupTopic.ControlGroup.parseFrom(data);
                        case "enigma.event.AdMarkup":
                            return AdMarkupTopic.AdMarkup.parseFrom(data);
                        case "enigma.event.DataRequest":
                            return AdRequestTopic.AdRequest.parseFrom(data);

                        default:
                            return null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

        });

        // print
        message.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

//    private Path getOutputPath() {
//
//    }

    public static void main(String[] args) throws Exception {
        //TODO: check the args

        process(args);
    }
}



