package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.LongWritable;

@Description(name = "simpleSum", value = "_FUNC_(num) - Returns the sum of integers")
public class SimpleSumUDAF extends UDAF {
    // 用UDAFEvaluator接口来规范实现方式
    public static class SumIntUDAFEvaluator implements UDAFEvaluator {
        // 初始化方法，用于将局部变量 partial 置为 null，以便开始新的聚合计算。
        @Override
        public void init() {
            partial = null;
        }

        // 存部分数据
        public static class PartialResult {
            long sum;
        }

        // UDAFEvaluator内部的局部变量，用于保存计算过程中的部分结果
        private PartialResult partial;

        // 构造函数，在构造实例时会调用父类的初始化方法 init()
        public SumIntUDAFEvaluator() {
            init();
        }

        // 迭代方法，用于接收每个输入值，并将其累加到部分结果 partial.sum 中
        public boolean iterate(LongWritable input) throws UDFArgumentException {
            if (input == null) {
                return true;
            }
            if (partial == null) {
                partial = new PartialResult();
            }
            partial.sum += input.get();
            return true;
        }

        // 部分结果输出方法，返回当前的部分结果 partial
        public PartialResult terminatePartial() {
            return partial;
        }

        // 合并方法，用于合并不同 Mapper 或 Reducer 的部分结果。它将其他部分结果 other 中的值合并到当前的部分结果 partial.sum 中
        public boolean merge(PartialResult other) {
            if (other == null) {
                return true;
            }
            if (partial == null) {
                partial = new PartialResult();
            }
            partial.sum += other.sum;
            return true;
        }

        // 最终结果输出方法，返回计算的最终结果。如果部分结果为空，返回 null；否则，返回计算的总和
        public LongWritable terminate() {
            if (partial == null) {
                return null;
            }
            return new LongWritable(partial.sum);
        }
    }
}

