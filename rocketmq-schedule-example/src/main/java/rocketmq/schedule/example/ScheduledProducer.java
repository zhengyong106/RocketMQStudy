package rocketmq.schedule.example;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class ScheduledProducer {
    public static void main(String args[]) throws Exception {
        // 通过producer group name初始化生产者实例
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        producer.setNamesrvAddr("192.168.10.10:9876");
        // 运行生产者实例
        producer.start();

        for(int i = 0; i < 100; i++){
            // 创建message实例, 指定topic, tag 和 message body
            Message msg = new Message("ScheduleTopic", "*", ("Hello RocketMQ " + i).getBytes());
            // "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
            // 设置消息延迟级别
            msg.setDelayTimeLevel(3);
            // 通过send方法发送消息用以将消息传递给broker
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
