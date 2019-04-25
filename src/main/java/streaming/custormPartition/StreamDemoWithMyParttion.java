package streaming.custormPartition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.custormSource.MyNoParalleSource;

/**
 * 使用自定义分区
 * 通过数字的奇偶性进行区分
 */
public class StreamDemoWithMyParttion {
    public static void main(String[] args ) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Long> text =  env.addSource(new MyNoParalleSource());
//        对数据进行转换，把Long转换成Tuple
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
//        分区之后的数据
        DataStream<Tuple1<Long>> partitionDate = tupleData.partitionCustom(new MyPartition(),0);

        DataStream<Long> result =  partitionDate.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                //              获取当前线程的
                System.out.println("当前线程ID" + Thread.currentThread().getId() + " value" + value);
                return  value.getField(0);
            }

        });
        result.print().setParallelism(1);
        String jobName = StreamDemoWithMyParttion.class.getName();
        env.execute(jobName);
    }
}
