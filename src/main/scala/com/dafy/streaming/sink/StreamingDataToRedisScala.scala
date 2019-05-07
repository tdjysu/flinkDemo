package com.dafy.streaming.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object StreamingDataToRedisScala {
  def main(args: Array[String]): Unit = {
//    获取运行环境
     val  env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost",6973,'\n')
    //    增加隐式转换
    import org.apache.flink.api.scala._
    val wordsData_1 = text.map(line=>("words_scala",line))

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper)

    wordsData_1.addSink(redisSink)

    env.execute("Socket window count")
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{
    override def getCommandDescription: RedisCommandDescription = {
       new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getValueFromData(data: (String, String)): String = {
          data._2
    }

    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }
  }
}
