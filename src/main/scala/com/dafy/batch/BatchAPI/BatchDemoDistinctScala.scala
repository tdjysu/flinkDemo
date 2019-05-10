package com.dafy.batch.BatchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoDistinctScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val dataList = ListBuffer[String]()
    dataList.append("Hello Flink")
    dataList.append("Hello Blink")
    dataList.append("Hello Spark")

    val text = env.fromCollection(dataList)

    val flatMapData = text.flatMap(line => {
      val words = line.split("\\W")
       for(word <- words){
         println("the value-> " + word)
       }
      words
    })
    flatMapData.distinct().print()
  }
}
