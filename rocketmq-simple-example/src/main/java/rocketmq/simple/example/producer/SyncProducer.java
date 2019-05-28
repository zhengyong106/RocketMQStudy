package rocketmq.simple.example.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 通过producer group name初始化生产者实例
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        producer.setNamesrvAddr("192.168.10.10:9876");
        // 运行生产者实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建message实例, 指定topic, tag 和 message body
            Message msg = new Message("TestTopic2", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 通过send方法发送消息用以将消息传递给broker
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        // 在生产者实例不再使用时关闭。
        producer.shutdown();
    }
}

