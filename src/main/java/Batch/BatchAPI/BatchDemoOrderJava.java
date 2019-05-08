package Batch.BatchAPI;


import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class BatchDemoOrderJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
         data1.add(new Tuple2<>(1,"tom"));
         data1.add(new Tuple2<>(2,"Jerry"));
         data1.add(new Tuple2<>(3,"albert"));

         ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();

         data2.add(new Tuple2<>(4,"USA"));
         data2.add(new Tuple2<>(4,"UK"));
         data2.add(new Tuple2<>(4,"CN"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        UnionOperator<Tuple2<Integer, String>> unionResult = text1.union(text2);

//        fist N按照数据接入的顺序
        unionResult.first(3).print();
        System.out.println("***************************************************************");
//        sort by column根据第一个元素进行排序
//先分组，再获取分组内的前两条数据
        unionResult.groupBy(0).first(2).print();
        System.out.println("***************************************************************");
//根据第一列分组，再根据第二列进行排序，获取前两个元素
        unionResult.groupBy(0).sortGroup(1, Order.DESCENDING).first(1).print();
        System.out.println("***************************************************************");
//        不分组，全局排序
        unionResult.sortPartition(0,Order.ASCENDING).first(5).print();
    }
}
