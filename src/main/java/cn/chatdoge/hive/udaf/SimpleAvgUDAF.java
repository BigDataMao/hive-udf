package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

public class SimpleAvgUDAF extends UDAF {
    public static class SimpleAvgUDAFEvaluator implements UDAFEvaluator{
        private  int sum;
        private  int count;
        @Override
        public void init() {
            sum = 0;
            count = 0;
        }

        public boolean iterate(IntWritable input){
            sum += input.get();
            count ++;
            return true;
        }

        public List<Integer> terminatePartial(){
//            a.add(sum);
//            a.add(count);
//            return a;
            List<Integer> partialResult = new ArrayList<Integer>();
            partialResult.add(sum);
            partialResult.add(count);
            return partialResult;
        }

        public boolean merge(List<Integer> other){
            sum = sum + other.get(0);
            count = count + other.get(1);
            return true;
        }

        public IntWritable terminate(){
            return new IntWritable(sum / count);
        }
    }
}
