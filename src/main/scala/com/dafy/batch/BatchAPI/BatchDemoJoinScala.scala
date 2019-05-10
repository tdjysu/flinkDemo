package com.dafy.batch.BatchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoJoinScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"USA"))
    data1.append((5,"UK"))
    data1.append((3,"CN"))
    val text1 = env.fromCollection(data1)

    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"Tom"))
    data2.append((2,"Jerry"))
    data2.append((3,"Albert"))
    val text2 = env.fromCollection(data2)
//apply用法
    text1.join(text2).where(0).equalTo(0).apply((first,second)=>{
      (first._1,first._2,second._2)
    }).print()
//Map用法
    text1.join(text2).where(0).equalTo(0).map((rs) => {
      (rs._1._1,rs._1._2,rs._2._2)
    }).print()
  }
}
