package com.dafy.streaming.streamAPI


import com.dafy.streaming.customerPartitioner.MyPartitionerScala
import com.dafy.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingMyPartitionerScala {
  def main(args:Array[String]):Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
//    增加隐式转换
    import org.apache.flink.api.scala._
     val text = env.addSource(new MyNoParallelSourceScala)
//把Long类型的数据转化为Tuple
     val tupleData =text.map(line=>{
       Tuple1(line)
     })

    val partitionData = tupleData.partitionCustom( new MyPartitionerScala,0)
    val result = partitionData.map(line=>{
      println("当前线程"+Thread.currentThread().getId+", value:"+line)
      line._1
    })

    result.print().setParallelism(2)
    val jobName = StreamingMyPartitionerScala.getClass.getName
    env.execute(jobName)
}
}
