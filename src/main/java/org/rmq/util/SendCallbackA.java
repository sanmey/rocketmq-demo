package org.rmq.util;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.rmq.async.AsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @Classname SendCallbackA
 * @description SendCallbackA
 * @author: wangyao
 * @create 2022/8/24 14:48
 */
public class SendCallbackA implements SendCallback {

    private static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    private Message message;

    private CountDownLatch countDownLatch;

    public SendCallbackA(Message message) {
        this.message = message;
        this.countDownLatch = new CountDownLatch(0);
    }

    public SendCallbackA(Message message, CountDownLatch countDownLatch) {
        this.message = message;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void onSuccess(SendResult sendResult) {
        String keys = message.getKeys();
        logger.info("{} 消息发送成功 {}", keys, sendResult.getMsgId());
        countDownLatch.countDown();
    }

    @Override
    public void onException(Throwable e) {
        String keys = message.getKeys();
        logger.error("{}  消息发送失败Exception", keys, e);
        countDownLatch.countDown();
    }
}
