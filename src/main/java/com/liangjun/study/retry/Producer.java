package com.liangjun.study.retry;


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

        //投递失败重试次数
        producer.setRetryTimesWhenSendFailed(3);
        //投递失败后是否允许投递到其他broker节点
        producer.setRetryAnotherBrokerWhenNotStoreOK(true);

        Message message = new Message("topic01", "Hi,Jun".getBytes());
        producer.send(message);
        System.out.println("消息已发送");

        producer.shutdown();
        System.out.println("生产端关闭");
    }
}