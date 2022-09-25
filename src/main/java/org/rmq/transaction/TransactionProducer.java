package org.rmq.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.rmq.util.Constant;
import org.rmq.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @Classname TransactionProducer
 * @description TransactionProducer
 * @author: wangyao
 * @create 2022/8/17 20:39
 */
public class TransactionProducer {

    static Random random = new Random();

    private static Logger logger = LoggerFactory.getLogger(TransactionProducer.class);


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
                String orderId = message.getKeys();
                //do TransactionA
                //do TransactionB
                boolean flag = random.nextBoolean();
                if (flag) {
                    localTransactionState = LocalTransactionState.COMMIT_MESSAGE;
                }
                localTransactionResult.put(orderId, localTransactionState);
                Map<String, Object> map = (Map) o;
                map.put(orderId, localTransactionState);
                if (random.nextBoolean()) {
                    //do Transaction Error
                    throw new RuntimeException("事务处理异常，状态未知");
                }
                return localTransactionState;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                LocalTransactionState state = localTransactionResult.getOrDefault(message.getKeys(), LocalTransactionState.UNKNOW);
                logger.info("回查本地事务状态:{},消息内容:{}", state, Utils.convertMsgStr(message));
                return state;
            }
        });
        // 启动生产者
        producer.start();

        // 发送10条消息
        for (int i = 0; i < 10; i++) {
            try {
                String orderId = String.valueOf(i);
                Map<String, Object> map = new HashMap<>();

                Message msg = new Message(Constant.TRANSACTION_TOPIC, "", ("Transaction Message" + i)
                        .getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 设置消息ID
                msg.setKeys(orderId);
                // 使用事务方式发送消息
                SendResult sendResult = producer.sendMessageInTransaction(msg, map);
                logger.info("消息ID: {},事务状态: {},SendStatus={}", orderId, map.get(orderId), sendResult.getSendStatus());
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                logger.error("第{}个消息发送异常", i, e);
            }
        }
        // 阻塞，目的是为了在消息发送完成后才关闭生产者
        Thread.sleep(100000);
        producer.shutdown();
    }

}
