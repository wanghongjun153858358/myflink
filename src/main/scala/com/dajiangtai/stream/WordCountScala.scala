package com.red.stream

import com.red.batch.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * Created by John on 2019/6/3
  */
object WordCountScala {

  def main(args: Array[String]): Unit = {
    //解析参数
    val params =  ParameterTool.fromArgs(args);

    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取输入数据
    val dataStream =
      if(params.has("input")){
        env.readTextFile(params.get("input"))
      }else{
        env.fromElements(WordCountData.WORDS: _*)
      }


    //数据处理
    val counts: DataStream[(String,Int)] = dataStream.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)


    //数据的输出
    if(params.has("output")){
      counts.writeAsText(params.get("output"))
    }else{
      counts.print()
    }

    //执行flink程序
    env.execute("Streaming WordCount")
  }

}
