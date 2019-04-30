package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import streaming.custormSource.MyNoParalleSource;

/**
 * 接收socket数据，把数据保存到redis中
 */
public class StreamingDemoToRedis {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost",8686,"\n");
        DataStream<Tuple2<String,String>> l_wordsData = text.map(new MapFunction<String,Tuple2<String,String>>() {
            public Tuple2<String,String> map(String value) throws Exception{
                return new Tuple2("l_words",value);
            }
        });
//        创建Redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
//        创建RedisSink
        RedisSink<Tuple2<String,String >> redisSink = new RedisSink<>(conf,new MyRedisMapper());

        l_wordsData.addSink(redisSink);
        String jobName= StreamingDemoToRedis.class.getName();
            env.execute(jobName);

    }

   public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
//表示从接收的数据中获取需要操作的Redis Key
        @Override
        public String getKeyFromData(Tuple2<String,String> data){
          return data.f0;
      }
//     表示从接收的数据中获取需要的Redis value
      @Override
        public String getValueFromData(Tuple2<String,String> data){
            return data.f1;
      }


    }

}
