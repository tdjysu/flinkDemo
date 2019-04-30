package com.dafy.streaming.streamAPI

import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoUnionScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text1 = env.addSource(new MyNoParallelSourceScala)
     val text2 = env.addSource(new MyNoParallelSourceScala)
    val unionText = text1.union(text2)
//针对map接收到的数据进行加1
    val sumData = unionText.map(line=> {
      println("接收到的数据-->" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sumData.print().setParallelism(2)
    val jobName = StreamingDemoUnionScala.getClass.getName
    env.execute(jobName)
}
}
