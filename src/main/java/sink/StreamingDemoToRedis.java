package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
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
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

//        new redisSink<Tuple2<String,String>>(conf,)
//        l_wordsData.addSink(new RedisSink)
        //    lpush

    }



}
