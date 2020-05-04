package com.red.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 累加器
 *
 * @author red
 * @create 2019-06-08-16:56
 */
public class WordCountCounter {

    public static void main(String[] args) throws Exception{
        //解析命令行传过来的参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取一个执行环境
        final ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();

        //读取输入数据
        DataSet<String> dataSet ;
        if(params.has("input")){
            dataSet = evn.readTextFile(params.get("input"));
        }else {
            dataSet= WordCountData.getDefaultTextLineDataSet(evn);
        }

        //单词词频统计
        DataSet<Tuple2<String, Integer>> counts = dataSet.flatMap(new WordCountCounter.Tokenizer())
                .groupBy(0)
                .sum(1);

        if(params.has("output")){
            //数据输出为csv 格式
            counts.writeAsCsv(params.get("output"),"\n"," ");
            //提交执行flink应用
            JobExecutionResult jobExecutionResult = evn.execute("wordcount example");
            int counter = jobExecutionResult.getAccumulatorResult("num-lines");
            System.out.println("counter="+counter);

        }else {
            //数据打印到控制台
            counts.print();
        }
    }

    public  static final class  Tokenizer extends RichFlatMapFunction<String, Tuple2<String,Integer>> {
        //1.创建累加器对象
        private IntCounter numLines = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //2.注册累加器对象
            getRuntimeContext().addAccumulator("num-lines", this.numLines);

        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //使用累加器统计数据行数
            this.numLines.add(1);
            String[] tokens = value.toLowerCase().split("\\W+");
            for(String token:tokens){
                out.collect(new Tuple2<>(token,1));
            }
        }
    }
}
