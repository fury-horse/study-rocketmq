package com.liangjun.study.sql;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
* @desc    基础的生产者
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

        for (int i=1; i<=10; i++) {
            Message message = new Message("topic01", ("Hello Jun " + i).getBytes());
            message.putUserProperty("age", String.valueOf(i));
            producer.send(message);
            System.out.println("消息已发送");
        }

        producer.shutdown();
        System.out.println("生产端关闭");
    }
}