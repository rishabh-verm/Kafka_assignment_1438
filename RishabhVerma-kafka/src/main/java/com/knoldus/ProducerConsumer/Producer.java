package com.knoldus.ProducerConsumer;

import com.knoldus.Model.UserModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.Serialization.UserDataSerializer");

        KafkaProducer<String, UserModel> kafkaProducer = new KafkaProducer<>(properties);
        try{
            UserModel userModel1 = new UserModel( 1 , "Rishabh" , 22 ,"BTECH");
            UserModel userModel2 = new UserModel(2,"rishabh",22,"MTECH");
            kafkaProducer.send(new ProducerRecord<>("UserData" , "First" , userModel1));
            kafkaProducer.send(new ProducerRecord<>("UserData" , "Second" , userModel2));
            System.out.println("Data sent");
            kafkaProducer.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
