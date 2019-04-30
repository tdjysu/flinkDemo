package com.dafy.streaming.streamAPI

import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoConnecterScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text1 = env.addSource(new MyNoParallelSourceScala)
     val text2 = env.addSource(new MyNoParallelSourceScala)
    val text2Str = text2.map("str"+ _)
    val connectedStream = text1.connect(text2Str)
    val result = connectedStream.map(line1=>(line1),line2=>(line2))

    result.print().setParallelism(2)
    val jobName = StreamingDemoConnecterScala.getClass.getName
    env.execute(jobName)
}
}
