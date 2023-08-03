package cn.chatdoge.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class AddUDF extends UDF{
    @Description(name = "add", value = "Returns the sum of two integers")
    public IntWritable evaluate(int col1, int col2){
        return new IntWritable(col1 + col2);
    }

}