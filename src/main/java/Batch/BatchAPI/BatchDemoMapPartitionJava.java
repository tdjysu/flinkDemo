package Batch.BatchAPI;



import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.Iterator;


public class BatchDemoMapPartitionJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> dataList = new ArrayList<String>();
        dataList.add("hello flink");
        dataList.add("hello spark");
        DataSource<String> text =  env.fromCollection(dataList);


/*        text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//            每过来一条数据，就获取一次连接
//获取数据库连接

                return value;
            }
        });*/


        DataSet<String> mapPartitiondata = text.mapPartition(new MapPartitionFunction<String,String>(){
           @Override
           public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
//               获取数据库连接，注此时是一个分区的数据只获取一个连接
//values中保存了一个分区的数据
             Iterator<String> it =  values.iterator();
             while(it.hasNext()){
                String next  = it.next();
                String[] splits=  next.split("\\W+");
                 for (String word:splits) {
                     out.collect(word);
                 }
             }

//处理数据
//               关闭连接

           }
       });
         mapPartitiondata.print();

    }
}

