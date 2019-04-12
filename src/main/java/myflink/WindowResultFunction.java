package myflink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public  class WindowResultFunction implements WindowFunction <Long, ItemViewCount, Tuple, TimeWindow>  {

//   key 窗口的主键 即itemId
//   window 窗口
//    聚合函数的接口，即count值
//    输出类型为ItemViewCount


    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> aggregateResult, Collector<ItemViewCount> collector) throws Exception {
        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}
