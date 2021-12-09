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
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;

public class ReduceJoin {
    public static class ReduceJoinMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 数据样例①：7369,SMITH,CLERK,7902,1980/12/17,800,,20
            // 数据样例②：10,accounting,new york
            String data = value.toString();
            // 分词
            String[] words = data.split(",");
            // 如果数据来自表一
            if (words.length == 8){
                // 部门编号， 部门工资
                System.out.println("数据一");
                context.write(new Text(words[7]), new Text(words[5]));
            }else if (words.length == 3){
                // 部门编号， 部门名称
                System.out.println("数据二");
                context.write(new Text(words[0]), new Text("$" + words[1]));
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        int count = 0;
        String Department_name = "";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text x: values){
                System.out.println("x：" + x);
                System.out.println(x.toString().indexOf("a"));
                if (x.toString().indexOf("$") != -1){
                    // 是表2的数据, 取得名字
                    Department_name = x.toString().substring(2);
//                    Department_name = x.toString();
                }else{
                    count = count + Integer.parseInt(x.toString());
                }
            }

            // 部门名称 部门总工资
            context.write(new Text(Department_name), new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "ReduceJoin"); // 实例化一道作业
        job.setJarByClass(ReduceJoin.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(ReduceJoinMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(ReduceJoinReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(Text.class); // 输出key的类型
        job.setOutputValueClass(IntWritable.class);// 输出value的类型


        Path inputPath1 = new Path(".\\src\\inputdata\\emp.csv");
        Path inputPath2 = new Path(".\\src\\inputdata\\dept.csv");
        Path outputPath = new Path(".\\src\\outputdata\\实验十ReduceJOIN");

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
        FileInputFormat.setInputPaths(job, inputPath1, inputPath2);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
