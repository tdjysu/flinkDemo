package com.dafy.batch.BatchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoMapPartitionScala {
  def main(args:Array[String]): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataList = ListBuffer[String]()
    dataList.append("Hello USA")
    dataList.append("Hello CN")

    val text = env.fromCollection(dataList)

    text.mapPartition(it=>{
//      创建数据库连接，建议增加try catch
      val res = ListBuffer[String]()
       while (it.hasNext){
         val line = it.next()
         val words = line.split("\\W")
         for(word <- words){
            res.append(word)
         }
       }
       res
//      关闭连接
    }).print()

  }


}
