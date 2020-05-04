package com.red.stream.window;
import com.red.batch.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理-单词词频统计
 *
 * @author red
 * @create 2019-06-02-21:51
 */
public class WordCount {
    public static void main(String[] args) throws  Exception{
        //解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //接受数据
        DataStream<String> dataStream;
        if(params.has("input")){
            dataStream= env.readTextFile(params.get("input"));
        }else{
            dataStream = env.fromElements(WordCountData.WORDS);
        }

        //数据处理，统计每个单词词频
        DataStream<Tuple2<String,Integer>> counts = dataStream.flatMap(new Tokenizer())
                .keyBy(0)
//                .countWindow(3)
//                .timeWindow(Time.seconds(10))
                .sum(1);

        //输出统计结果
        if(params.has("output")){
            counts.writeAsText(params.get("output"));
        }else{
            counts.print();
        }


        //执行flink程序
        env.execute("Streaming WordCount");
    }

    public  static final class  Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for(String token:tokens){
                out.collect(new Tuple2<>(token,1));
            }
        }
    }
}
