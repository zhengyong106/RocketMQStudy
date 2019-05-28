package rocketmq.order.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class OrderedConsumer {
    public static void main(String[] args) throws Exception {
        // 通过consumer group name初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-consumer-group");
        // 指定nameServer服务地址
        consumer.setNamesrvAddr("192.168.10.10:9876");
        // 指定consumer从何处开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 为consumer订阅一个或者多个topic
        consumer.subscribe("OrderTopic", "TagA");

        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
                        for (MessageExt msg: msgs) {
                            System.out.println("message body:" + new String(msg.getBody()) + "\tmessage keys:" + msg.getKeys());
                        }
                        return ConsumeOrderlyStatus.SUCCESS;
                    });

        // 运行消费者者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}