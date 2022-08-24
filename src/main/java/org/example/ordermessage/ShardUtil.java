package org.example.ordermessage;

import org.apache.rocketmq.common.message.Message;
import org.example.async.AsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Classname ShardUtil
 * @description ShardUtil
 * @author: wangyao
 * @create 2022/8/24 15:09
 */
public class ShardUtil {

    private static Logger logger = LoggerFactory.getLogger(ShardUtil.class);

    public static int getShardIndex(int total, Message msg) {
        String keys = msg.getKeys();
        int hashCode = keys.hashCode();
        int index = Math.abs(hashCode % total);
//        logger.info("消息ID={}路由队列->{}",keys,index);
        return index;
    }
}
