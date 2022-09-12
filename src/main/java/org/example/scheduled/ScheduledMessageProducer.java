package org.example.scheduled;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.example.util.Constant;

/**
 * @Classname ScheduledMessageProducer
 * @description ScheduledMessageProducer
 * @author: wangyao
 * @create 2022/8/24 17:12
 */


public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer("ScheduledMessageProducerA");
        producer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        // 启动生产者
        producer.start();
        int totalMessagesToSend = 100;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message(
                    Constant.SCHEDULED_TOPIC,
                    "", String.valueOf(i),
                    ("Hello scheduled message " + i).getBytes());
            //messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(1);
            // 发送消息
            producer.send(message);
        }
        // 关闭生产者
        producer.shutdown();
    }
}
