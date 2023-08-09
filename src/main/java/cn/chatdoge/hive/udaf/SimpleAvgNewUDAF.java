package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class SimpleAvgNewUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new myEvaluator();
    }

    public static class myEvaluator extends GenericUDAFEvaluator {

        private IntObjectInspector inputOI; // 输入类型的ObjectInspector
        private IntWritable result; // 结果类型

        static class AvgAgg extends AbstractAggregationBuffer {
            int sum;
            int count;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // 初始化输入类型的ObjectInspector
            inputOI = (IntObjectInspector) parameters[0];

            // 初始化结果类型
            result = new IntWritable(0);

            // 返回结果类型的ObjectInspector
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AvgAgg avgAgg = new AvgAgg();
            reset(avgAgg);
            return avgAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            avgAgg.sum = 0;
            avgAgg.count = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null) {
                AvgAgg avgAgg = (AvgAgg) agg;

                // 将输入值添加到聚合结果中
                avgAgg.sum += inputOI.get(parameters[0]);
                avgAgg.count++;
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;

            // 将输入值添加到聚合结果中
            avgAgg.sum += inputOI.get(partial);
            avgAgg.count++;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;

            // 设置最终的聚合结果
            result.set(avgAgg.sum / avgAgg.count);
            return result;
        }
    }
}
