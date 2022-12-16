package com.test.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerWithGracefulShutDown {
  private static final Logger log = Logger.getLogger(ConsumerDemo.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am in kafka consumer demo");
    //creating consumer configuration
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-first-application");
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"); //none/earliest/latest
    //create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

    //get a reference to the current thread
    final Thread currentThread = Thread.currentThread();

    //adding shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        log.info("Detected a shutdwon, let's exit by calling consumer.wakeup()...");
        consumer.wakeup();
        try{
          currentThread.join();
        }catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
    });
    try {
      //For one topic subsribtion
      consumer.subscribe(Arrays.asList("test.topic"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          log.info("Key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
      }
    }catch (WakeupException e){
      log.info("Wakeup exception caught");
    }
    catch (Exception e){
      log.info("Unexpected exception caught");
    }finally {
      consumer.close();
      log.info("Consumer gracefully shut down");
    }
  }
}
