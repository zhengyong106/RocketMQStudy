package rocketmq.broadcasting.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

public class BroadcastConsumer {
    public static void main(String[] args) throws Exception {
        // 通过consumer group name初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast-consumer-group");
        // 指定nameServer服务地址
        consumer.setNamesrvAddr("192.168.10.10:9876");
        // 指定consumer从何处开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 设置广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 为consumer订阅一个或者多个topic
        consumer.subscribe("BroadcastTopic", "TagA || TagC || TagD");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.out.printf("Broadcast Consumer Started.%n");
    }
}
