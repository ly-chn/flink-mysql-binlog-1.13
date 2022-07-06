package kim.nzxy;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Liaoliao
 */
public class ProcessForLogMysqlChange extends KeyedProcessFunction<Integer, String, Object> {

    @Override
    public void processElement(String value, KeyedProcessFunction<Integer, String, Object>.Context ctx, Collector<Object> out) throws Exception {
        System.out.println("value = " + value);
    }
}
