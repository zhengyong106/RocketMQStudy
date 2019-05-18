package rocketmq.order.example;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OrderedProducer {
    public static void main(String args[]) throws Exception{
        // 通过producer group name初始化生产者实例
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        producer.setNamesrvAddr("192.168.10.10:9876");
        // 运行生产者实例
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 100; i++) {
            Message message = new Message("OrderTopic", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ " + tags[i % tags.length]).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(message, (mqs, msg, arg) -> {
                Integer id = (Integer) arg;
                int index = id % mqs.size();
                return mqs.get(index);
            }, i % tags.length);

            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
