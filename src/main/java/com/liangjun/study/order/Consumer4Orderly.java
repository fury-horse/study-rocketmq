package com.liangjun.study.order;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
* @desc    顺序消费消息
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:55:55
**/
public class Consumer4Orderly {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cGroup01");
        consumer.setNamesrvAddr("HOST01:9876");

        //订阅topic
        consumer.subscribe("topic01", "*");
        //设置最大消费线程数
        consumer.setConsumeThreadMax(10);
        //设置最小消费线程数
        consumer.setConsumeThreadMin(10);

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                for (MessageExt msg : list) {
                    System.out.println(
                            "收到消息：" + new String(msg.getBody()) +
                            ", queue:" + msg.getQueueId() +
                            ", thread:" + Thread.currentThread().getName()
                    );
                }
                //消息ACK
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.out.println("消费端启动");
    }
}