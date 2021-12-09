package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class template {
    public static class templateMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        public void map(Object key, Text value, Context context){
            // 数据样例：
        }
    }

    public static class templateReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context){

        }
    }

    public static void main(String[] args) throws Exception{

    }
}
