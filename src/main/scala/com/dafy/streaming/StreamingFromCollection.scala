package com.dafy.streaming

import java.util

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object StreamingFromCollection {
  @throws[Exception]
  def main(args: Array[String]): Unit = { //      获取Flink的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataList = new util.ArrayList[Integer]
    dataList.add(9)
    dataList.add(16)
    dataList.add(86)
    //指定数据源
    val collectionData = env.fromCollection(dataList)
    //通过map对数据进行处理
    val num = collectionData.map(new MapFunction[Integer, Integer]() {
      @throws[Exception]
      override def map(`val`: Integer): Integer = `val` + 1
    })
    num.print.setParallelism(1)
    env.execute("StriamingFromCollection Test")
  }
}