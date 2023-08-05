package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

public class SimpleAvgUDAF extends UDAF {
    public static class SimpleAvgUDAFEvaluator implements UDAFEvaluator{
        private static int sum;
        private static int count;
        private static List<Integer> a;
        @Override
        public void init() {
            sum = 0;
            count = 0;
            List<Integer> a = new ArrayList<Integer>();
        }

        public boolean iterate(IntWritable input){
            sum += input.get();
            count ++;
            return true;
        }

        public List<Integer> terminatePartial(){
            a.add(sum);
            a.add(count);
            return a;
        }

        public boolean merge(List<Integer> other){
            int aSum = sum + other.get(0);
            int aCount = count + other.get(1);
            a.set(0, aSum);
            a.set(1, aCount);
            return true;
        }

        public IntWritable terminate(){
            return new IntWritable(a.get(0) / a.get(1));
        }
    }
}
