package com.liangjun.study.model;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
* @desc    集群消费模式
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:55:55
**/
public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cGroup01");
        consumer.setNamesrvAddr("HOST01:9876");

        //订阅topic
        consumer.subscribe("topic01", "*");
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

        //集群模式
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //广播模式
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.start();
        System.out.println("消费端启动");
    }
}