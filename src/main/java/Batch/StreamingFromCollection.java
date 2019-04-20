package Batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;


public class StreamingFromCollection {
    public static void main (String[] args) throws Exception{
//      获取Flink的env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> dataList = new ArrayList<>();
        dataList.add(9);
        dataList.add(16);
        dataList.add(86);
//指定数据源
        DataStreamSource<Integer> collectionData = env.fromCollection(dataList);
//通过map对数据进行处理
        DataStream<Integer> num = collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer val) throws Exception {
                return val + 1;
            }
        });

        num.print().setParallelism(1);
        env.execute("StriamingFromCollection Test");

    }
}
