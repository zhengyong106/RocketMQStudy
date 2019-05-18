package rocketmq.batch.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class BatchConsumer {
    public static void main(String args[]) throws MQClientException {
        // 通过consumer group name初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batch-consumer-group");
        // 指定nameServer服务地址
        consumer.setNamesrvAddr("192.168.10.10:9876");
        // 为consumer订阅一个或者多个topic
        consumer.subscribe("BatchTopic", "*");
        // 设置consumer每次最多拉取100条
        consumer.setConsumeMessageBatchMaxSize(100);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 打印接收到的消息总数
                System.out.println("Receive Messages count: " + msgs.size());
                for(MessageExt msg: msgs){
                    // 打印当前线程id以及接收到的messageId
                    System.out.printf(Thread.currentThread().getName() + " Receive New Message: " + msg.getMsgId() + "%n");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 运行消费者者实例
        consumer.start();
    }
}
