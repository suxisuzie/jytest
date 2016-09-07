package com.xad.jytest.sstest;

import com.google.protobuf.ByteString;
import com.xad.enigma.EnigmaEventFramework;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import com.xad.enigma.core.EnigmaFramework;
//import com.xad.enigma.eventmodel.core.Topic;
import com.google.protobuf.Message;

import org.apache.avro.protobuf.ProtobufDatumWriter;
import org.apache.hadoop.fs.Path;
import com.xad.enigma.EnigmaEventFramework.EnigmaEnvelope;
import java.util.*;
import com.xad.enigma.AdDetailsTopic.AdDetails;
import com.xad.enigma.*;
import com.xad.enigma.AdDemandPartnerReportingRequestTopic.AdDemandPartnerReportingRequest;
import com.xad.enigma.AdDemandPartnerReportingRequestTopic.AdDemandPartnerReportingRequest;
import com.xad.enigma.AdDetailsTopic.AdDetails;
import com.xad.enigma.AdRequestTopic.AdRequest;
import com.xad.enigma.AdTrackingTopic.AdTracking;
import com.xad.enigma.AdUserProfileTopic.AdUserProfile;
import com.xad.enigma.DelUserProfileTopic.DelUserProfile;
import com.xad.enigma.RTITrackingTopic.RTITracking;
import com.xad.enigma.SampleTrackingTopic.SampleTracking;
import com.xad.enigma.HttpVendorStatsTopic.HttpVendorStats;
import com.xad.enigma.SegmentBuilderTopic.SegmentBuilder;
import com.xad.enigma.UserTrackingTopic.UserTracking;
import com.xad.enigma.BlockAttributesTopic.BlockAttributes;
import com.xad.enigma.AdDocumentTopic.AdDocument;
import com.xad.enigma.RTITrackingTopic.RTITracking;
import com.xad.enigma.AtlanticMetaDataTopic.AtlanticMetaData;
import com.xad.enigma.VisitTrackingTopic.VisitTracking;
import com.xad.enigma.VisitTrackingTopic.AdExposure;
import com.xad.enigma.ControlGroupTopic.ControlGroup;
import com.xad.enigma.AdMarkupTopic.AdMarkup;

import groovy.util.logging.Log4j;

@Log4j

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
    public final static String EXT = ".avro";
    private static void process(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("jytest-ss");
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
        final DataFileWriter<Object> writer = new DataFileWriter<>(new ProtobufDatumWriter<>());
        Path path = getOutputPath();
        JavaDStream<Message> message = directStream.map(new Function<Tuple2<byte[], EnigmaEnvelope>, Message>() {
            @Override
            public Message call(Tuple2<byte[], EnigmaEnvelope> tuple2) {
                EnigmaEnvelope envelope  = tuple2._2();
                Topic topic = Topic.fromName(envelope.getEventTopic());
                Class<com.google.protobuf.Message> protoClass = Topic.fromName(envelope.getEventTopic()).protoClass;
                Schema schema = ProtobufData.get().getSchema(protoClass);

                try{
                    Message record = STest1.parseFromEnvelope (topic, envelope.getEventData());
                    return record;
                } catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });



//        directStream.foreachRDD(new Function<JavaPairRDD<byte[], EnigmaEnvelope>>() {
//            @Override
//            public Void call(JavaPairRDD<byte[], EnigmaEnvelope> enigmaEnvelopeJavaPairRDD) throws Exception {
//                return enigmaEnvelopeJavaPairRDD.foreach(new Function<Tuple2<byte[], EnigmaEnvelope>>() {
//                    @Override
//                    public void call(Tuple2<byte[], EnigmaEnvelope> enigmaEnvelopeTuple2) throws Exception {
//                        EnigmaEnvelope envelope = enigmaEnvelopeTuple2._2();
//                        if(!envelope.hasIsHeartbeat()) {
//                            Class protoClass = Topic.fromName(envelope.getEventTopic()).protoClass;
//                            System.out.println(protoClass.toString());
//                        }
//
//
//                    }
//                });
//            }
//        });


//        directStream.map(message->)



        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
    public static Message parseFromEnvelope(Topic topic, ByteString  eventDate) throws com.google.protobuf.InvalidProtocolBufferException{
        switch (topic) {
            case AD_DEMAND_PARTNER_REPORTING_REQUEST: AdDemandPartnerReportingRequest.parseFrom(eventDate);
            case AD_REQUEST: AdRequest.parseFrom(eventDate);
            case AD_DETAILS: AdDetails.parseFrom(eventDate);
            case AD_TRACKING: AdTracking.parseFrom(eventDate);
            case AD_USER_PROFILE: AdUserProfile.parseFrom(eventDate);
            case UPDATE_USER_PROFILE: AdUserProfile.parseFrom(eventDate);
            case DEL_USER_PROFILE: DelUserProfile.parseFrom(eventDate);
            case SAMPLE_TRACKING: SampleTracking.parseFrom(eventDate);
            case HTTP_VENDOR_STATS:HttpVendorStats.parseFrom(eventDate);
            case SEGMENT_BUILDER: SegmentBuilder.parseFrom(eventDate);
            case USER_TRACKING: UserTracking.parseFrom(eventDate);
            case BLOCK_ATTRIBUTES: BlockAttributes.parseFrom(eventDate);
            case AD_DOCUMENT: AdDocument.parseFrom(eventDate);
            case RTI_TRACKING: RTITracking.parseFrom(eventDate);
            case ATLANTIC_METADATA: AtlanticMetaData.parseFrom(eventDate);
            case VISIT_TRACKING: VisitTracking.parseFrom(eventDate);
            case AD_EXPOSURE: AdExposure.parseFrom(eventDate);
            case AD_EXPOSURE_STATE: AdExposure.parseFrom(eventDate);
            case CONTROL_GROUP: ControlGroup.parseFrom(eventDate);
            case  AD_MARKUP: AdMarkup.parseFrom(eventDate);
            case DATA_REQUEST: AdRequest.parseFrom(eventDate);

        }
    }
    private Path getOutputPath() {

    }
    public static void main(String[] args) throws Exception {
        //TODO: check the args

        process(args);
    }
}



