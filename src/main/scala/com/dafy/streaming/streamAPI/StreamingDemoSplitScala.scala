package com.dafy.streaming.streamAPI

import java.util.ArrayList
import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingDemoSplitScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text = env.addSource(new MyNoParallelSourceScala)

     val splitStream =  text.split(new OutputSelector[Long] {
       override def select(value: Long) = {
          val list = new java.util.ArrayList[String]()
         if(value % 2 == 0){
           list.add("even")
         }else{
           list.add("odd")
         }
         list

       }
     })

   val result = splitStream.select("even")
    result.print().setParallelism(2)
    val jobName = StreamingDemoSplitScala.getClass.getName
    env.execute(jobName)
}
}
