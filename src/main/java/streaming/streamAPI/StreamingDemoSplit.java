package streaming.streamAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.custormSource.MyParalleSource;

import java.util.ArrayList;

/**
 * 根据规则把一个数据流切分成多个流
 */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception{

//       获取Flink的运行环境\
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源,默认并行度为电脑cup核数
        DataStreamSource text = env.addSource(new MyParalleSource()).setParallelism(2);

        SplitStream splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> out =  new ArrayList();
                if(value % 2 == 0){
                    out.add("even");
                }else {
                    out.add("odd");
                }
                return out;
            }
        });

//        选择一个或者多个切分的数据流
           DataStream<Long> evenStream = splitStream.select("even");
           DataStream<Long> oddStream = splitStream.select("odd");
//           选择多个流做为分流结果
           DataStream<Long> moreStream = splitStream.select("even","odd");





//        打印结果moreStream
        moreStream.print();
        String jobNmae = StreamingDemoSplit.class.getName();
        env.execute(jobNmae);
    }

}
