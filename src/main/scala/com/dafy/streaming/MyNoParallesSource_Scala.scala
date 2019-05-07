package com.dafy.streaming

import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object MyNoParallesSource_Scala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text = env.addSource(new MyNoParallelSourceScala)
//针对map接收到的数据进行加1
    val mapData = text.map(line=> {
      println("scala 接收到的数据" + line)
      line
    })
    val sumData = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sumData.print().setParallelism(2)
    val jobName = MyNoParallesSource_Scala.getClass.getName
    env.execute(jobName)
}
}
