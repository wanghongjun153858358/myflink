package com.red.stream.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.red.stream.userPurchaseBehaviorTracker.model.EvaluatedResult;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * 计算结果序列化类
 *
 * @author red
 * @create 2019-06-24-13:31
 */
public class EvaluatedResultSerializationSchema implements KeyedSerializationSchema<EvaluatedResult> {
    @Override
    public byte[] serializeKey(EvaluatedResult element) {
        return element.getUserId().getBytes();
    }

    @Override
    public byte[] serializeValue(EvaluatedResult element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public String getTargetTopic(EvaluatedResult element) {
        return null;
    }
}
