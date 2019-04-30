package com.dafy.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object  BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text:DataSet[String] = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")
//    text.print()
val counts = text.flatMap {
      _.toLowerCase.split(" ") filter {
        _.nonEmpty
      }
    }
      .map {(_, 1)}
      .groupBy(0)
      .sum(1).setParallelism(1)

    counts.print()

  }
}
