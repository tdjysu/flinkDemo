package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.custormSource.MyNoParalleSource;

/**
 * 使用并行度为1的Source
 */
public class StreamingDemoWithMyNoParaleelSource {
    public static void main(String[] args) throws Exception{

//       获取Flink的运行环境\
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源
//        注意针对此source,只能设计并行度为1
        DataStreamSource text = env.addSource(new MyNoParalleSource());
        DataStream<Long> num = text.map(new MapFunction<Long,Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据" + value);
                return value;
            }
        });
//        每2秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
//        打印结果
        sum.print().setParallelism(2);
        String jobNmae = StreamingDemoWithMyNoParaleelSource.class.getName();
        env.execute(jobNmae);
    }

}
