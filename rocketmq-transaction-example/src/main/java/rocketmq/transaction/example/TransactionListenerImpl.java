package rocketmq.transaction.example;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {
    private final Random random = new Random();

    private ConcurrentHashMap<String, Integer> hashMap = new ConcurrentHashMap<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        Integer count = 0;
        hashMap.put(msg.getTransactionId(), count);

        System.out.printf("threadName: %s, messageId: %s, retryCount: %s, transactionState: %s%n", Thread.currentThread().getName(), msg.getTransactionId(), count, "UNKNOW");
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Integer count = hashMap.get(msg.getTransactionId())+ 1;
        hashMap.put(msg.getTransactionId(), count);

        switch (random.nextInt(3)) {
            case 0:
                System.out.printf("threadName: %s, messageId: %s, retryCount: %s, transactionState: %s%n", Thread.currentThread().getName(),msg.getTransactionId(), count, "UNKNOW");
                return LocalTransactionState.UNKNOW;
            case 1:
                System.out.printf("threadName: %s, messageId: %s, retryCount: %s, transactionState: %s%n", Thread.currentThread().getName(),msg.getTransactionId(), count, "ROLLBACK_MESSAGE");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            case 2:
                System.out.printf("threadName: %s, messageId: %s, retryCount: %s, transactionState: %s%n", Thread.currentThread().getName(),msg.getTransactionId(), count, "COMMIT_MESSAGE");
                return LocalTransactionState.COMMIT_MESSAGE;
            default:
                System.out.printf("threadName: %s, messageId: %s, retryCount: %s, transactionState: %s%n", Thread.currentThread().getName(),msg.getTransactionId(), count, "COMMIT_MESSAGE");
                return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
