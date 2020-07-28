package com.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @RequestMapping("/send")
    public String send(){
        final String uuid = UUID.randomUUID().toString();
        kafkaTemplate.send(KafkaCommon.SPRING_BOOT_KAFKA_TOPIC, uuid);
        return "success";
    }


}
