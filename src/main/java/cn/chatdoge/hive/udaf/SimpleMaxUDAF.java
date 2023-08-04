package cn.chatdoge.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class SimpleMaxUDAF extends UDAF {
    public static class a implements UDAFEvaluator{
        @Override
        public void init() {
            int max = 0;
        }


    }
}
