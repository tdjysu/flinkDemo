package Batch.BatchAPI;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 左外连接
 * 右外连接
 * 全外连接
 */
public class BatchDemoOutJoinJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
         data1.add(new Tuple2<>(1,"tom"));
         data1.add(new Tuple2<>(2,"Jerry"));
         data1.add(new Tuple2<>(3,"albert"));

         ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();

         data2.add(new Tuple2<>(1,"USA"));
         data2.add(new Tuple2<>(2,"UK"));
         data2.add(new Tuple2<>(4,"CN"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


//        left join
//second
        /*text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second == null)
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,"unknow");
                        else
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);

                    }
                }).print();*/

//right join
/*        text1.rightOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {

                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null)
                            return new Tuple3<Integer, String, String>(second.f0,"unknowName",second.f1);
                        else
                            return new Tuple3<Integer, String, String>(second.f0,first.f1,second.f1);
                    }
                }).print();*/

//        full outer join

        text1.fullOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null ){
                            return new Tuple3<Integer, String, String>(second.f0,"unknowName",second.f1);
                        }else if (second == null){
                            return  new Tuple3<Integer, String, String>(first.f0,first.f1,"unknow");
                        }else {
                            return  new Tuple3<Integer, String, String>(second.f0,first.f1,second.f1);
                        }
                    }
                }).print();

//通过Map方法,这里用Map和上面用with的结果是一致的
/*        text1.join(text2).where(0).equalTo(0).map(new MapFunction<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>, Tuple3<Integer,String,String>>() {

            @Override
            public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                return new Tuple3<>(value.f0.f0,value.f0.f1,value.f1.f1);
            }
        }).print();*/
    }
}
