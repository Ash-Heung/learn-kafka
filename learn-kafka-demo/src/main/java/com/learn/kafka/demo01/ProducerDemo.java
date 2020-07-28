package com.learn.kafka.demo01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName : ProducerDemo
 * @Description :
 * @Author : xpf
 * @Date: 2020-05-07 11:12
 */
public class ProducerDemo {
    public final static String TOPIC = "test_first";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.101:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC,"message"+i));
            TimeUnit.SECONDS.sleep(3);
        }

        producer.close();
    }
}
