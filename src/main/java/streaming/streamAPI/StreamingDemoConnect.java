package streaming.streamAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import streaming.custormSource.MyParalleSource;

public class StreamingDemoConnect {


    public static void main(String args[]) throws Exception {
       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

       //        获取数据源,默认并行度为电脑cup核数
       DataStreamSource<Long> text1 = env.addSource(new MyParalleSource()).setParallelism(2);
       DataStreamSource<Long> text2 = env.addSource(new MyParalleSource()).setParallelism(2);

       SingleOutputStreamOperator<String>  textStr = text2.map(new MapFunction<Long, String>() {
           @Override
           public String map(Long value) throws Exception {
               return "str_" + value;
           }
       });

      ConnectedStreams<Long,String> connectStream = text1.connect(textStr);

        SingleOutputStreamOperator result = connectStream.map(new CoMapFunction<Long, String, Object>() {
          @Override
          public Object map1(Long value) throws Exception {
              return value;
          }

          @Override
          public Object map2(String s) throws Exception {
              return s;
          }
      });
        result.print().setParallelism(1);
        String jobName = StreamingDemoConnect.class.getName();
        env.execute(jobName);
   }
}
