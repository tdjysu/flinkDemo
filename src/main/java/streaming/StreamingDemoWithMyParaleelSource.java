package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.custormSource.MyParalleSource;

/**
 * 使用多行度的Source
 */
public class StreamingDemoWithMyParaleelSource {
    public static void main(String[] args) throws Exception{

//       获取Flink的运行环境\
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源,默认并行度为电脑cup核数
        DataStreamSource text = env.addSource(new MyParalleSource()).setParallelism(2);
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
        sum.print();
        String jobNmae = StreamingDemoWithMyParaleelSource.class.getName();
        env.execute(jobNmae);
    }

}
