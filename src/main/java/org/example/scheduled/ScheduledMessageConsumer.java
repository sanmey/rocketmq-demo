package org.example.scheduled;

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
/**
 * @Classname ScheduledMessageConsumer
 * @description ScheduledMessageConsumer
 * @author: wangyao
 * @create 2022/8/24 17:11
 */


import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScheduledMessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(ScheduledMessageConsumer.class);
    public static void main(String[] args) throws Exception {
        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ScheduledMessageConsumerA");
        consumer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        // 订阅Topics
        consumer.subscribe(Constant.SCHEDULED_TOPIC, "*");
        // 注册消息监听者
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                List<Map<String, Object>> list = messages.stream().map(Utils::convertMsgStr).collect(Collectors.toList());
                logger.info("接收到新消息: {}", JSONObject.toJSONString(list));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
    }
}
