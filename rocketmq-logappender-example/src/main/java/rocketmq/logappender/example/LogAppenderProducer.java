//package rocketmq.logappender.example;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Date;
//
//public class LogAppenderProducer {
//
//    private static Logger logger = LoggerFactory.getLogger(LogAppenderProducer.class);
//
//    public static void main(String args[]) throws InterruptedException {
//        for (int i = 0; i < 10; i++) {
//            Thread.sleep(1000);
//            logger.info(new Date().toString());
//        }
//    }
//}
