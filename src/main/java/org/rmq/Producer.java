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
package org.rmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.rmq.util.Constant;
import org.rmq.util.SnowFlakeIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

import static org.rmq.util.Constant.COMMON_TOPIC;

public class Producer {

    private static Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(
            String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {

        DefaultMQProducer producer = new DefaultMQProducer("async_producer_groupA");
        producer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();
        int messageCount = 6;
        for (int i = 0; i < messageCount; i++) {
            try {
                String id = String.valueOf(SnowFlakeIdGenerator.getInstance().nextId());
                Message msg = new Message(COMMON_TOPIC,
                        "TagA", id
                        , ("Hello world" + id).getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
                logger.info("{} 消息发送成功 {}", msg.getKeys(), msg);
            } catch (Exception e) {
                logger.error("消息发送失败", e);
            }
        }
        producer.shutdown();
    }
}
