package com.liangjun.study.order;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
* @desc    并发消费消息
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:55:55
**/
public class Consumer4Concurrent {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cGroup02");
        consumer.setNamesrvAddr("HOST01:9876");

        //订阅topic
        consumer.subscribe("topic01", "*");
        //设置最大消费线程数
        //consumer.setConsumeThreadMax(10);
        //设置最小消费线程数
        //consumer.setConsumeThreadMin(10);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : list) {
                    System.out.println(
                            "收到消息：" + new String(msg.getBody()) +
                            ", queue:" + msg.getQueueId() +
                            ", thread:" + Thread.currentThread().getName()
                    );
                }
                //消息ACK
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("消费端启动");
    }
}