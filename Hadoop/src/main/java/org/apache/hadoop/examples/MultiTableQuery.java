package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/*      1.求各个部门的总工资，要求输出的数据内容为“部门名称 部门总工资”；  <部门编号， 部门总工资>
        2.求各个部门的人数和平均工资，要求输出的数据内容为“部门名称 部门人数 部门平均工资”；
        3.求每个部门最早进入公司的员工，要求输出的数据内容为“部门名称 最早入职的员工姓名 入职日期”。*/

public class MultiTableQuery {
    public static class templateMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 员工编号，员工姓名，职位，领导的员工编号，雇佣日期，工资，奖金，部门编号   部门编号，部门名称，部门所在地点
            // 数据样例：7369,SMITH,CLERK,7902,1980/12/17,800,,20    10,accounting,new york
            String data = value.toString();
            String[] words = data.split(",");
            if (words.length == 8){
                context.write(new IntWritable(Integer.parseInt(words[5])), new IntWritable(Integer.parseInt(words[7])));
            } else{
                context.write(new IntWritable(Integer.parseInt(words[5])), new IntWritable(Integer.parseInt(words[7])));
            }
        }
    }

    public static class templateReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context){

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "多表查询");
        job.setJarByClass(MultiTableQuery.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(Serializable.SalaryTotalMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Serializable.Employee.class);

        // !!!!!!!!!指定分区规则
        job.setPartitionerClass(Serializable.SalaryParitioner.class);
        // !!!!!!!!!指定建立几个分区
        job.setNumReduceTasks(3);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(Serializable.SalaryTotalReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(IntWritable.class);// 输出key的类型， 部门号
        job.setOutputValueClass(IntWritable.class);// 输出value的类型， 员工

        // 集群
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 本地测试
        FileInputFormat.setInputPaths(job, new Path("E:\\Test\\Hadoop\\src\\inputdata\\emp.csv"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Test\\Hadoop\\src\\inputdata\\ten"));

        // 执行任务
        job.waitForCompletion(true);

    }
}
