package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 实现一个求中位数的UDAF
 *
 * @author simon.mau
 * @date 2023/8/3
 */
@Description(
        name = "median",
        value = "_FUNC_(x) - Returns the median value of a numeric column"
)
public class MedianUDAF extends UDF {
    public static class MedianUDAFEvaluator implements UDAFEvaluator{
        private List<Double> buffer;

        // TODO 1/6 初始化
        @Override
        public void init() {
            buffer = new ArrayList<>();
        }

        // TODO 2/6 迭代
        public boolean iterate(Double value){
            if (value != null){
                buffer.add(value);
            }
            return true;
        }

        // TODO 3/6 Map阶段返回结果
        public AbstractAggregationBuffer terminatePartial(){
            MedianBuffer resultBuffer = new MedianBuffer();
            resultBuffer.buffer = new ArrayList<>(buffer);
            return resultBuffer;
        }

        // TODO 4/6 Reduce阶段聚合
        public boolean merge(AbstractAggregationBuffer other){
            if (other != null){
                MedianBuffer otherBuffer = (MedianBuffer) other;
                buffer.addAll(otherBuffer.buffer);
            }
            return true;
        }

        // TODO 5/6 reset可选

        // TODO 6/6 最终结果
        public Double terminate(){
            if (buffer.isEmpty()){
                return null;
            }

            Collections.sort(buffer);
            int size = buffer.size();
            int midIndex = size / 2;
            if (size % 2 == 0){
                return (buffer.get(midIndex -1) + buffer.get(midIndex)) / 2;
            } else {
                return buffer.get(midIndex);
            }
        }

    }

    public static class MedianBuffer extends AbstractAggregationBuffer{
        List<Double> buffer;
    }
}
