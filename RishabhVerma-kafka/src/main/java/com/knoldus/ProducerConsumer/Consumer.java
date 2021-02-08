package com.knoldus.ProducerConsumer;

import com.knoldus.Model.UserModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    
    public static void main(String[] args) {
       ConsumerListener c = new ConsumerListener();
       Thread thread = new Thread(c);
       thread.start();
    }
      public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.Serialization.UserDataDeSerializer");
        properties.put("group.id", "test-group");

        
        KafkaConsumer<String, UserModel> kafkaConsumer = new KafkaConsumer<>(properties);
        List arrayList = new ArrayList();
        arrayList.add("UserData");
        kafkaConsumer.subscribe(arrayList);

        try{
            while (true){
                ConsumerRecords<String, UserModel> records = kafkaConsumer.poll(Duration.ofMillis(100));
                FileWriter fileWriter = new FileWriter("Data.txt" , true);
                ObjectMapper objectMapper = new ObjectMapper();
                for (ConsumerRecord<String, UserModel> record: records){
                    System.out.println(record.value().getName());
                    // method that would be writing this value to a file.
                    System.out.println("data received");
                    System.out.println(objectMapper.writeValueAsString(record.value()));
                    fileWriter.append(objectMapper.writeValueAsString(record.value())+"\n");
                }
                fileWriter.close();
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
            System.out.println("Consumer Closed");
        }
    }
}

class ConsumerListener implements Runnable {
    
    
    @Override
    public void run() {
    Consumer.consumer();
    }
}