package org.example.transaction;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.example.util.Constant;
import org.example.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Classname TransactionProducer
 * @description TransactionProducer
 * @author: wangyao
 * @create 2022/8/17 20:39
 */
public class TransactionConsumer {

    private static Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    public static void main(String[] args) throws Exception {

        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction-consumer-group");

        // 设置NameServer的地址
        consumer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        // 订阅  ,订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe(Constant.TRANSACTION_TOPIC, "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                List<Map<String, Object>> list = msgs.stream().map(Utils::convertMsgStr).collect(Collectors.toList());
                logger.info("接收到新消息: {}", JSONObject.toJSONString(list));
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
