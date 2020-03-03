package com.learning.kafka.tutorial1;

import com.sun.xml.internal.ws.api.client.SelectOptimalEncodingFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers="127.0.0.1:9092";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        //send data
        ProducerRecord<String,String> record= new ProducerRecord<String,String>("first_topic","Welcome to first topic");

        producer.send(record);
        //It won't work without flush and close as producer is asynchronous

        //flush only
        producer.flush();

        //flush and close
        producer.close();

    }
}
