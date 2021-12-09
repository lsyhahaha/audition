package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class SecondSort {
    public static class SecondSortMapper extends Mapper<Object, Text, IntWritable, Data> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 数据样例：1010037	100
            // 数据样例：
            String data = value.toString();
            // 分词
            String[] values = data.trim().split("\\s+");
            Data e = new Data();
            e.set(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
            // 输出
            context.write(new IntWritable(Integer.parseInt(values[1])), e);
        }
    }

    public static class SecondSortReducer extends Reducer<IntWritable, Data, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Data> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> array = new ArrayList();
            for (Data x : values){
                array.add(x.getFirst());
            }
            int len = array.size();
            for (int i = 0; i < len; i++){
                for (int j = len - 2; j >= 0; j--){
                    if (array.get(j) < array.get(j+1)){
                        int t = array.get(j);
                        array.set(j, array.get(j + 1));
                        array.set(j+1, t);
                    }
                }
            }

            for (int x: array){
                context.write(key, new IntWritable(x));
            }
        }
    }

    //使Sort阶段的Key降序排列的比较器
    public static class IntWritableDecreasingComparator extends IntWritable.Comparator{
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SecondOrder"); // 实例化一道作业
        job.setJarByClass(SecondSort.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(SecondSortMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SecondSort.Data.class);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(SecondSortReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(IntWritable.class);// 输出key的类型
        job.setOutputValueClass(IntWritable.class);// 输出value的类型

        //设置Sort阶段使用比较器
        job.setSortComparatorClass(IntWritableDecreasingComparator.class);


        Path inputPath = new Path(".\\src\\inputdata\\SecondOrder.txt");
        Path outputPath = new Path(".\\src\\outputdata\\实验九SecondSort");

        // if outputPAth exist
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)){
            // 文件存在，删除该文件
            fs.delete(outputPath, true);
        }

        // 集群测试
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        // 本地测试
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Data extends IntWritable{
        int first;
        int second;

        /**
         * Set the left and right values.
         */
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        // 反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            first = in.readInt();
            second = in.readInt();
        }

        @Override
        // 序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeInt(first);
            out.writeInt(second);
        }
    }
}
