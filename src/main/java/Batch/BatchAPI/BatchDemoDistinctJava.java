package Batch.BatchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class BatchDemoDistinctJava {
    public static void main(String[] args) throws Exception {
//        获取离线数据环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> dataList = new ArrayList<String>();
        dataList.add("flink");
        dataList.add("durid");
        dataList.add("hdfs");
        dataList.add("flink");

        DataSource<String> text = env.fromCollection(dataList);

        FlatMapOperator<String, String> flatmapdata = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.toLowerCase().split("\\W+");
                for (String word : splits) {
                    System.out.println("value is " + word);
                    out.collect(word);
                }
            }
        });
//数据进行整体
        flatmapdata.distinct().print();
    }
}
