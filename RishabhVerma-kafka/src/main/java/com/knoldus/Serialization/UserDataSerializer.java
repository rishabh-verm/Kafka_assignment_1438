package com.knoldus.Serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *  String encoding defaults to UTF8 and can be customized by setting the property key.serializer.encoding,
 *  value.serializer.encoding or serializer.encoding. The first two take precedence over the last.
 */
public class UserDataSerializer implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Object object) {
        byte [] data= null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            data = objectMapper.writeValueAsString(object).getBytes();
        }
        catch (Exception e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " );
        }
        return data;

    }
    @Override
    public void close(){

    }
}
