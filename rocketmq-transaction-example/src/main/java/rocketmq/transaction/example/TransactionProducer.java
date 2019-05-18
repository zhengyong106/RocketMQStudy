package rocketmq.transaction.example;

import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProducer {
    private static AtomicInteger atomicInteger = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        // 通过producer group name初始化生产者实例
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        // 指定nameServer服务地址
        producer.setNamesrvAddr("192.168.10.10:9876");
        // 创建事务监听
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);

        // 创建线程池
        ExecutorService executorService = new ThreadPoolExecutor(100, 100, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("transaction-producer-thread" + TransactionProducer.atomicInteger.incrementAndGet());
                return thread;
            }
        });
        producer.setExecutorService(executorService);
        // 运行生产者实例
        producer.start();

        for (int i = 0; i < 10; i++) {
            // 创建message实例, 指定topic, tag 和 message body
            Message msg = new Message("TransactionTopic", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 通过send方法发送消息用以将消息传递给broker
            producer.sendMessageInTransaction(msg, null);
        }
    }
}
