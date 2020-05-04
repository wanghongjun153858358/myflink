package com.red.stream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 自定义source
 *
 * @author red
 * @create 2019-06-09-16:36
 */
public class CountForMySource {

    public static void main(String[] args) throws Exception{
        //解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dataStream = env.addSource(new SimpleSourceFunction());

        DataStream<Long> numDataStream = dataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("数据源="+value);
                return value;
            }
        });

        AllWindowedStream<Long, TimeWindow> longTimeWindowAllWindowedStream = numDataStream.timeWindowAll(Time.seconds(5), Time.seconds(5));
        SingleOutputStreamOperator<Long> sum1 = longTimeWindowAllWindowedStream.sum(0);
        sum1.print().setParallelism(1);
        System.out.println("-----------------------------------------");

//        DataStream<Long> sum = numDataStream.timeWindowAll(Time.seconds(5)).sum(0);
//
//
//        sum.print().setParallelism(1);
//
//
//        Iterator<Long> myOutput = DataStreamUtils.collect(sum);
//        while (myOutput.hasNext()){
//            System.out.println("测试sink"+myOutput.next());
//        }


        env.execute("red");


    }

    public  static  class SimpleSourceFunction implements ParallelSourceFunction<Long> {
        private long num = 1L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning){
                sourceContext.collect(num);
//                num++;
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
