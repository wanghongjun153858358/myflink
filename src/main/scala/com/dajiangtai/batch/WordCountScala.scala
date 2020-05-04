package com.red.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
  * Created by John on 2019/6/3
  */
object WordCountScala {

  def main(args: Array[String]): Unit = {

    //解析参数
    val params:ParameterTool = ParameterTool.fromArgs(args)

    //获取执行环境
   val env =  ExecutionEnvironment.getExecutionEnvironment

    //获取输入数据
    val dataSet =

      if(params.has("input")){
        env.readTextFile(params.get("input"))
      }else{
        env.fromCollection(WordCountData.WORDS)
      }


    //数据处理
    val counts = dataSet.flatMap{ _.toLowerCase.split("\\W+")filter{ _.nonEmpty}}
        .map{ (_,1)}
        .groupBy(0)
        .sum(1)

    //数据输出
    if(params.has("output")){
      counts.writeAsCsv(params.get("output"),"\n"," ")
      env.execute("scala WordCount example")
    }else{
      counts.print()
    }


  }

}
