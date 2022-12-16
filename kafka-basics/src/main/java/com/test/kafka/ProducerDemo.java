package com.test.kafka;

import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
  private static final Logger log = Logger.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) {
    log.info("Hello world!");
    //Create producer properties
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
    //create a producer record
    ProducerRecord<String, String> record = new ProducerRecord<String, String>("test.topic", "Hello World");
    //send the data - asynchronous to the producer
    producer.send(record);
    //close the producer
    producer.flush();
    producer.close();
  }
    //send data

    //flush and close the producer
  }

