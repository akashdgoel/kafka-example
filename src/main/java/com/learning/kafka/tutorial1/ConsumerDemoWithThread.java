package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
    new ConsumerDemoWithThread().run();

    }
        //consumer.subscribe(Collections.singleton(topic));
        private ConsumerDemoWithThread(){

        }
        private void run() {
            Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
            String boostrapServers = "127.0.0.1:9092";
            String groupId = "my_application";
            String topic = "first-topic";
            CountDownLatch latch = new CountDownLatch(1);
            logger.info("Creating the consumer thread");
            Runnable myConsumerRunnable = new ConsumerRunnable(boostrapServers, groupId, topic, latch);
            //Start the thread
            Thread myThread = new Thread(myConsumerRunnable);
            myThread.start();
            //add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                logger.info("Caught shutdown hook");
                ((ConsumerRunnable) myConsumerRunnable).shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.error("Application got interrupted",e);
                } finally {
                    logger.info("Application is closing");
                }
            }));
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted",e);
            } finally {
                logger.info("Application is closing");
            }
        }


    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String boostrapServers, String groupId, String topic,CountDownLatch latch){
            this.latch=latch;
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + " Value : " + record.value());
                    }
                }

            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                //Tell main code to exit
                latch.countDown();
            }
        }

        public void shutdown(){
            //It is a special method to interrupt consumer.poll
            //It will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}
