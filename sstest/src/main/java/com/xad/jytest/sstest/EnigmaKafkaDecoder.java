package com.xad.jytest.sstest;

import com.xad.enigma.EnigmaEventFramework.EnigmaEnvelope;
import kafka.serializer.Decoder;

import java.io.IOException;

/**
 * Created by suziesu on 9/6/16.
 */
public class EnigmaKafkaDecoder implements Decoder<EnigmaEnvelope> {

    @Override
    public EnigmaEnvelope fromBytes(byte[] bytes) {
        try {
            EnigmaEnvelope evenlop = EnigmaEnvelope.parseFrom(bytes);
            if (!evenlop.hasIsHeartbeat()) {
                return evenlop;
            }
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
