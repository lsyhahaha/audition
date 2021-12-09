package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopScore {

    public static class AccessSortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text course = new Text();
        IntWritable score = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 数据样例：
            String data = value.toString();
            // 分词
            String[] values = data.trim().split(" ");
            // Mapper阶段数据整理
            course.set(values[0]);
            score.set(Integer.parseInt(values[1])); // 将字符转换为int类型，再将score设置为该值

            // 输出
            context.write(course, score);
        }
    }

    public static class AccessSortReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxScore = -1;
            Text course = new Text();
            for (IntWritable score:values){
                if (score.get() > maxScore){
                    maxScore = score.get();
                    course = key;
                }
            }

            context.write(course, new IntWritable(maxScore));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "TopScore"); // 实例化一道作业
        job.setJarByClass(TopScore.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(AccessSortMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(AccessSortReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(Text.class); // 输出key的类型
        job.setOutputValueClass(IntWritable.class);// 输出value的类型

        // 是否引入合并操作
        job.setCombinerClass(AccessSortReducer.class); // TopScore的reduce

        Path inputPath = new Path(".\\src\\inputdata\\score.txt");
        Path outputPath = new Path(".\\src\\outputdata\\实验五TopScore");

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
}