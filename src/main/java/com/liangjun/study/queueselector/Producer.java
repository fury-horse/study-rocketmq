package com.liangjun.study.queueselector;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;

/**
* @desc    生产者选择queue
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
            Message message = new Message("topic08", ("Hi,Jun" + i).getBytes());
            producer.send(message, new SelectMessageQueueByHash(), 1);
        }
        System.out.println("消息已发送");

        producer.shutdown();
        System.out.println("生产端关闭");
    }
}