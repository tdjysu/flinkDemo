package streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;



public class MyNoParalleSource implements SourceFunction<Long> {

    private boolean isRunning = true;
    private long count = 1L;


    /**
     * 主要的方法，启动一个source，大部分情况下，都需要在这个run方法中实现一个循环，这样就可以不断产生数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning){
            ctx.collect(count);
            count++;
//            每秒产生1条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancle的时候会调用的方法
     */

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
