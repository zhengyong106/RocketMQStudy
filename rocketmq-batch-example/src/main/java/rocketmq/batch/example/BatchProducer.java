package rocketmq.batch.example;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

public class BatchProducer {
    public static void main(String args[]) throws Exception {
        // 通过producer group name初始化生产者实例
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        producer.setNamesrvAddr("192.168.10.10:9876");
        // 运行生产者实例
        producer.start();

        // 创建MessageList
        List<Message> msgs = new ArrayList<>();
        for(int i = 0; i < 100; i++){
            msgs.add(new Message("BatchTopic", "TagA", ("Hello RocketMQ " + i).getBytes()));
        }

        // RocketMq建议一个批量消息的大小最好不要大于1MiB，因此使用消息分割器对消息列表进行分割
        MessageListSplitter splitter = new MessageListSplitter(msgs);
        while (splitter.hasNext()) {
            try {
                List<Message>  listItem = splitter.next();
                // 发送分割后的消息列表
                SendResult sendResult = producer.send(listItem);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 在生产者实例不再使用时关闭。
        producer.shutdown();
    }
}
