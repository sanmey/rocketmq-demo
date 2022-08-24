package org.example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.example.util.Constant;
import org.example.util.Util;
import org.example.domain.OrderPaidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @Classname TransactionProducer
 * @description TransactionProducer
 * @author: wangyao
 * @create 2022/8/17 20:39
 */
public class TransactionOrderProducer {

    static Random random = new Random();

    static String MSG_ID_KEY = "MSG_ID";

    static String TOPIC = "TransactionTopic";

    private static Logger logger = LoggerFactory.getLogger(TransactionOrderProducer.class);


    public static void main(String[] args) throws Exception {
        Map<String, LocalTransactionState> localTransactionResult = new HashMap<>();

        // 创建事务类型的生产者
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer-group");
        // 设置NameServer的地址
        producer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        // 设置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                LocalTransactionState localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                message.getKeys();
                if (o instanceof String) {
                    String mqId = (String) o;
                    //do TransactionA
                    //do TransactionB
                    boolean flag = random.nextBoolean();
                    if (flag) {
                        localTransactionState = LocalTransactionState.COMMIT_MESSAGE;
                    }
                }
                localTransactionResult.put(message.getProperty(MSG_ID_KEY), localTransactionState);
                if (random.nextBoolean()) {
                    throw new RuntimeException("事务处理异常，状态未知");
                }
                return localTransactionState;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                LocalTransactionState state = localTransactionResult.getOrDefault(message.getProperty(MSG_ID_KEY), LocalTransactionState.UNKNOW);
                logger.info("回查本地事务状态:{},消息内容:{}", state, message);
                // 需要根据业务，查询本地事务是否执行成功，这里直接返回COMMIT
                return state;
            }
        });
        // 启动生产者
        producer.start();

        // 发送10条消息
        for (int i = 0; i < 10; i++) {
            try {
                //MessageBuilder.withPayload(payload).build()
                String mqId = UUID.randomUUID().toString();

                Message msg = new Message(TOPIC, "", Util.toMessageBody(
                        new OrderPaidEvent(UUID.randomUUID().toString(), new BigDecimal(i)))
                );
                // 设置消息ID
                msg.putUserProperty(MSG_ID_KEY, mqId);
                // 使用事务方式发送消息
                SendResult sendResult = producer.sendMessageInTransaction(msg, mqId);
                SendStatus sendStatus = sendResult.getSendStatus();
                logger.info("第{}个消息，sendResult={}", i, sendResult);
                Thread.sleep(10);
            } catch (Exception e) {
                logger.error("第{}个消息发送异常", i, e);
            }
        }

        // 阻塞，目的是为了在消息发送完成后才关闭生产者
        Thread.sleep(100000);
        producer.shutdown();
    }

}
