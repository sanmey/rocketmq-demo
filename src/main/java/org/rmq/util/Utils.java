package org.rmq.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.message.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Classname Utils
 * @description Utils
 * @author: wangyao
 * @create 2022/8/23 18:43
 */
public class Utils {

    public static Map<String, Object> convertMsgStr(Message message) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("orderId", message.getKeys());
        map.put("body", new String(message.getBody()));
        return map;
    }

    public static String toJSONString(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String result = null;
        try {
            result = objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static <T> T toObject(String str, Class<T> valueType) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        T value = null;
        try {
            value = objectMapper.readValue(str, valueType);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return value;
    }

    public static byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;

    }

    public static Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return obj;

    }


    public static byte[] toMessageBody(Object obj) {
        byte[] bytes = new byte[0];
        if (flag) {
            bytes = toByteArray(obj);
        } else {
            bytes = toJSONString(obj).getBytes(StandardCharsets.UTF_8);
        }
        //System.out.println(bytes.length);
        return bytes;
    }

    private static boolean flag = false;
}
