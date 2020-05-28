package com.liangjun.study.transaction;


import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
* @desc    事务消息
* @version 1.0
* @author  Liang Jun
* @date    2020年04月23日 10:41:07
**/
public class Producer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("pGroup02");
        producer.setNamesrvAddr("HOST01:9876");

        //设置事务消息的监听器
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                //RocketMQ框架在回调当前方法前会首先发送到broker一条HF待确认消息
                try {
                    //执行本地业务
                    System.out.println("message:" + new String(message.getBody()));
                    System.out.println("本地业务执行成功");
                } catch (Exception e) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                //提交事务
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                //broker对处理中状态的消息会进行回调当前方法进行消息状态验证
                //执行本地验证
                System.out.println("message:" + new String(messageExt.getBody()));

                try {
                    //模拟验证过程
                    String msgId = messageExt.getMsgId();
                    Object record = "sql:select * from tb_message where msg_id = " + msgId;
                    //本地有投递记录则提交消息，否则回滚消息
                    return (null == record) ?
                            LocalTransactionState.ROLLBACK_MESSAGE :
                            LocalTransactionState.COMMIT_MESSAGE;
                } catch (Exception e) {
                    //异常无法确地本地事务是否正常完结，后续再试
                    return LocalTransactionState.UNKNOW;
                }
            }
        });

        producer.start();
        System.out.println("生产端启动");
        //构造消息
        Message message = new Message();
        message.setTopic("topic02");
        message.setBody("Hi,Jun".getBytes());
        //发送消息
        producer.sendMessageInTransaction(message, null);
        System.out.println("消息已发送");

        //维持生成端连接提供回调服务
        //producer.shutdown();
        System.out.println("生产端关闭");
    }
}