package com.liangjun.study.batch;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Arrays;

/**
* @desc    单向消息发送
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:41:07
**/
public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pGroup01");
        producer.setNamesrvAddr("HOST01:9876");
        producer.start();
        System.out.println("生产端启动");

        //批量发送消息
        producer.send(Arrays.asList(
                new Message("topic01", "Hello Jun 01".getBytes()),
                new Message("topic01", "Hello Jun 02".getBytes()),
                new Message("topic01", "Hello Jun 03".getBytes())
        ));
        System.out.println("消息发送成功");

        producer.shutdown();
        System.out.println("生产端关闭");
    }
}