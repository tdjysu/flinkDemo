package Batch.BatchAPI;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class BatchDemoUnionJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
         data1.add(new Tuple2<>(1,"tom"));
         data1.add(new Tuple2<>(2,"Jerry"));
         data1.add(new Tuple2<>(3,"albert"));

         ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();

         data2.add(new Tuple2<>(4,"USA"));
         data2.add(new Tuple2<>(5,"UK"));
         data2.add(new Tuple2<>(6,"CN"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        UnionOperator<Tuple2<Integer, String>> unionResult = text1.union(text2);

        unionResult.print();
    }
}
