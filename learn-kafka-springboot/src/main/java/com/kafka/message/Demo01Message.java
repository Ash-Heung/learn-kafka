package com.kafka.message;

import lombok.Data;

@Data
public class Demo01Message {
    public static final String TOPIC = "DEMO_01";

    /**
     * 编号
     */
    private Integer id;
}
