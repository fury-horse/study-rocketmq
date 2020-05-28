package com.liangjun.study.async;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
* @desc    异步消息发送
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

        Message message = new Message("topic01", "Hello Jun".getBytes());

        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息发送成功");
                System.out.println("sendResult : " + sendResult);
            }
            @Override
            public void onException(Throwable throwable) {
                throwable.printStackTrace();
                System.out.println("消息发送异常");
            }
        });

        //异步发送，不能太早关闭
        //producer.shutdown();
        System.out.println("生产端关闭");
    }
}