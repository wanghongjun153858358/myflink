package com.red.stream.kafkaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * Flink Kafka生产者
 *
 * @author red
 * @create 2019-06-18-9:23
 */
public class StreamingKafkaProducer {
    public static void main(String[] args) throws Exception{
        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("192.168.28.128",9999,"\n");

        String brokerList = "cdh01:9092";
        String topic = "test";

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);

        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(topic,new SimpleStringSchema(),prop);
        dataStream.addSink(myProducer);

        env.execute("StreamingKafkaProducer");
    }
}
