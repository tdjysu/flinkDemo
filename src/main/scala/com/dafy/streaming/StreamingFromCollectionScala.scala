package com.dafy.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingFromCollectionScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
    val data = List(10,15,20)
    val text = env.fromCollection(data)
//针对map接收到的数据进行加1
    val num = text.map(_+1)
    num.print().setParallelism(2)
    val jobName = StreamingFromCollectionScala.getClass.getName
    env.execute(jobName)
}
}
