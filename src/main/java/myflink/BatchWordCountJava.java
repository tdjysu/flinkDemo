package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;


public class BatchWordCountJava {
    public static void main(String[] args) throws Exception{

//  获取运行时环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        String inputPath = "C:\\test";
        String outPatch = "C:\\data\\output";
 //       获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);

        DataSet<Tuple2<String,Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outPatch,"\n",",");
        env.execute("batch word count");
    }


    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split(" ");
            for(String token:tokens){
                if(token.length() > 0){
                    out.collect(new Tuple2<String,Integer>(token, 1));
                }
            }
        }
    }

}
