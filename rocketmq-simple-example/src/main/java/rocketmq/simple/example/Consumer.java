package rocketmq.simple.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 通过consumer group name初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        consumer.setNamesrvAddr("192.168.10.10:9876");
        // 为consumer订阅一个或者多个topic
        consumer.subscribe("TopicTest", "*");

        // 注册回调函数,当从brokers获取到返回消息时执行.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 运行消费者者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}