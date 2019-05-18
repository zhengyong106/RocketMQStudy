package rocketmq.schedule.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class ScheduledConsumer {
    public static void main(String args[]) throws MQClientException {
        // 通过consumer group name初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("schedule-consumer-group");
        // 指定nameServer服务地址
        consumer.setNamesrvAddr("192.168.10.10:9876");
        // 为consumer订阅一个或者多个topic
        consumer.subscribe("ScheduleTopic", "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                // 粗略打印延迟时长
                System.out.println("Receive message[msgId=" + msg.getMsgId() + "] "+ (System.currentTimeMillis() - msg.getBornTimestamp()) + "ms later");
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 运行消费者者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
