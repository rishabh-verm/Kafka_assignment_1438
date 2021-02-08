package com.knoldus.Serialization;

import com.knoldus.Model.UserModel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * String encoding defaults to UTF8 and can be customized by setting the property key.deserializer.encoding,
 * value.deserializer.encoding or deserializer.encoding. The first two take precedence over the last.
 */
public class UserDataDeSerializer implements Deserializer  {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        UserModel userModel = null;
        try {
            userModel = objectMapper.readValue(data, UserModel.class);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userModel;

    }
}
