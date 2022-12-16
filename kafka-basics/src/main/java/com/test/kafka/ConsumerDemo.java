package com.test.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemo {
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
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-application");
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"); //none/earliest/latest
    //create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
    //For one topic subsribtion
    consumer.subscribe(Collections.singletonList("test.topic"));

    while (true) {
      log.info("Polling");

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        log.info("Key: " + record.key()+ ", Value: " + record.value());
        log.info("Partition: " + record.partition()+ ", Offset: " + record.offset());
      }
    }
  }
}
