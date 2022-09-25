/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rmq.ordermessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.rmq.util.Constant;
import org.rmq.util.SnowFlakeIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderProducer {

    private static Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    public static void main(String[] args) throws UnsupportedEncodingException {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("order_msg_producer_groupA");
            producer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
            producer.start();

            String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
            int count = Integer.MAX_VALUE;
            for (int i = 0; i < count; i++) {
                try {
                    long orderId = SnowFlakeIdGenerator.getInstance().nextId();
                    String id = String.valueOf(orderId);
                    Message msg = new Message(Constant.ORDER_TOPIC, tags[i % tags.length], id,
                            ("RocketMQ Order Msg " + id).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            MessageQueue queue = mqs.get(ShardUtil.getShardIndex(mqs.size(), msg));
                            logger.info("消息ID= {} 路由队列-> {}", msg.getKeys(), queue.getQueueId());
                            return queue;
                        }
                    }, null);
                    if (i % 10 == 0) {
                        Thread.sleep(2000);
                    }
                    Thread.sleep(1000);
                } catch (Exception e) {
                    logger.error("生产消息{}异常", i, e);
                }
            }
            producer.shutdown();
        } catch (Exception e) {
            logger.error("生产消息异常", e);
        }
    }
}
