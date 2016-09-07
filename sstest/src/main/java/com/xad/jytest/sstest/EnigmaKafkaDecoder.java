package com.xad.jytest.sstest;

import com.xad.enigma.EnigmaEventFramework.EnigmaEnvelope;
import kafka.serializer.Decoder;
import com.xad.enigma.eventmodel.core.Topic;
import java.lang.*;
/**
 * Created by suziesu on 9/6/16.
 */
public class EnigmaKafkaDecoder implements Decoder<EnigmaEnvelope> {
    @Override
    public EnigmaEnvelope fromBytes(byte[] bytes) {
        try{
            EnigmaEnvelope evenlop = EnigmaEnvelope.parseFrom(bytes);
            if(!evenlop.hasIsHeartbeat()) {
                return evenlop;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
