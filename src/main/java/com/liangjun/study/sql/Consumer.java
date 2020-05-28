package com.liangjun.study.sql;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
* @desc    基础消费端
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:55:55
**/
public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cGroup01");
        consumer.setNamesrvAddr("HOST01:9876");

        //订阅topic，过滤sql
        MessageSelector selector = MessageSelector.bySql("age > 2 and age < 7");
        consumer.subscribe("topic01", selector);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : list) {
                    System.out.println("收到消息：" + new String(msg.getBody()));
                }
                //消息ACK
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("消费端启动");
    }
}