package Batch.BatchAPI;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 笛卡尔
 */
public class BatchDemoCrossJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         ArrayList<String> data1 = new ArrayList<>();
         data1.add("tom");
         data1.add("Jerry");


         ArrayList<String> data2 = new ArrayList<>();

         data2.add("USA");
         data2.add("UK");
         data2.add("CN");

        DataSource<String> text1 = env.fromCollection(data1);
        DataSource<String> text2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String, String> crossResult = text1.cross(text2);
        crossResult.print();
    }
}
