package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;

public class SimpleMaxUDAF extends UDAF {
    public static class a implements UDAFEvaluator{

        private long max = 0;
        @Override
        public void init() {
            max = 0;
        }

        public a() {
            init();
        }

        public boolean iterate(LongWritable input){
            if (input == null){
                return true;
            }
            if (input.get() > max){
                max = input.get();
            }
            return true;
        }

        public long terminatePartial() {
            return max;
        }

        public boolean merge(long other){
            if (other > max){
                max = other;
            }
            return true;
        }

        public LongWritable terminate(){
            return new LongWritable(max);
        }
    }
}
