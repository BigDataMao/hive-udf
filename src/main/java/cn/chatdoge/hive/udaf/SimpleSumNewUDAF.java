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

public class SimpleSumNewUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return new GenericUDAFMySumInt();
    }

    public static class GenericUDAFMySumInt extends GenericUDAFEvaluator{

        private IntObjectInspector inputOI; // 输入类型的ObjectInspector
        private IntWritable result; // 结果类型

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

        static class SumAgg implements AggregationBuffer {
            int sum;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumAgg agg = new SumAgg();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumAgg sumAgg = (SumAgg) agg;
            sumAgg.sum = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null) {
                SumAgg sumAgg = (SumAgg) agg;

                // 将输入值添加到聚合结果中
                sumAgg.sum += inputOI.get(parameters[0]);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumAgg sumAgg = (SumAgg) agg;

                // 合并局部聚合结果到总聚合结果中
                sumAgg.sum += inputOI.get(partial);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumAgg sumAgg = (SumAgg) agg;

            // 设置最终的聚合结果
            result.set(sumAgg.sum);
            return result;
        }

    }
}
