package com.dafy.streaming.streamAPI

import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoFilterScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text = env.addSource(new MyNoParallelSourceScala)
//针对map接收到的数据进行加1
    val mapData = text.map(line=> {
      println("scala 原始接收到的数据" + line)
      line
    }).filter(_ % 2 == 0)
    val sumData = mapData.map(line=> {
      println("Filter处理之后的数据 "+ line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)
    sumData.print().setParallelism(2)
    val jobName = StreamingDemoFilterScala.getClass.getName
    env.execute(jobName)
}
}
