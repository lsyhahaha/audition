package org.apache.hadoop.examples.ten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.ReduceJoin;
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
import java.util.Scanner;

public class TotalJoin {
    public static class ReduceJoinMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 数据样例①：7369,SMITH,CLERK,7902,1980/12/17,800,,20
            // 数据样例②：10,accounting,new york
            String data = value.toString();
            // 分词
            String[] words = data.split(",");

            // 实例化
            Department d = new Department();
            Employee e = new Employee();
            // 如果数据来自表一
            if (words.length == 8){
                // 部门编号， 部门工资
                // 设置员工属性,依次为 员工编号，员工姓名，职位，领导的员工编号，雇佣日期，工资，奖金，部门编号
                e.setEmpno(Integer.parseInt(words[0]));
                e.setEname(words[1]);
                e.setJob(words[2]);

                try {
                    e.setMgr(Integer.parseInt(words[3])); // 可能为空
                }catch (Exception ex){
                    // 没有老板
                    e.setMgr(-1);
                }
                e.setHiredate(words[4]);
                // 月薪
                e.setSal(Integer.parseInt(words[5]));
                try{
                    e.setComm(Integer.parseInt(words[6]));
                }catch (Exception ex){
                    // 没有奖金
                    e.setComm(0);
                }
                e.setDeptno(Integer.parseInt(words[7]));
                System.out.println("mapper:" + e.toString());
                context.write(new Text(words[7]), new Text(e.toString()));
            }else if (words.length == 3){
                // 部门编号， 部门名称
                // 设置部门属性
                d.setDeptno(Integer.parseInt(words[0]));
                d.setDeptname(words[1]);
                d.setAddress(words[2]);
                System.out.println("reduce:" + d.toString());
                context.write(new Text(words[0]), new Text(d.toString()));
            }
        }
    }

    public static class JoinOneReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String Department_name = "";

            for (Text x: values){
                String[] words = x.toString().split(",");

//                System.out.println("x：" + x);
//                System.out.println(x.toString().indexOf("a"));
                if (words.length == 3){
                    // 是表2的数据, 取得名字
                    Department_name = words[1];
                }else if (words.length == 9){
                    count = count + Integer.parseInt(words[5]) + Integer.parseInt(words[6]);
                }
            }

            // 部门名称 部门总工资
            context.write(new Text(Department_name), new IntWritable(count));
        }
    }

    public static class JoinTwoReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;  // 部门工资
            String Department_name = ""; // 部门名称
            int total = 0; // 部门人数

            for (Text x: values){
                String[] words = x.toString().split(",");

//                System.out.println("x：" + x);
//                System.out.println(x.toString().indexOf("a"));
                if (words.length == 3){
                    // 是表2的数据, 取得名字
                    Department_name = words[1];
                }else if (words.length == 9){
                    count = count + Integer.parseInt(words[8]);
                    total = total  + 1;
                }
            }

            // 如果部门人数为0， 防止后面 by zero, 该情况直接退出
            if (total == 0){
                return;
            }
            // 部门名称      部门人数 部门平均工资
            context.write(new Text(Department_name), new Text(String.valueOf(total) + " " + String.valueOf(count / total)));
        }
    }

    public static class JoinThreeReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String Department_name = ""; // 部门名称
            String name = "lishiyong"; // 最早入职的员工姓名
            String date = "2021/12-09"; // 最早入职人的姓名

            for (Text x: values){
                String[] words = x.toString().split(",");
//                System.out.println("x：" + x);
//                System.out.println(x.toString().indexOf("a"));
                if (words.length == 3){
                    // 是表2的数据, 取得名字
                    Department_name = words[1];
                }else if (words.length == 9){
                    String cur_date = words[4]; // 当前这个人的入职时间

                    if (check(cur_date, date)){
                        // cur_date 早于 date
                        date = cur_date;
                        name = words[1];
                    }
                }
            }

            // 部门名称 最早入职的员工姓名 入职日期
            context.write(new Text(Department_name), new Text(name + " " + date));
        }

        private boolean check(String cur_date, String date) {
            // 用于判断哪个时间早
            String[] cur = cur_date.split("/");
            String[] data = date.split("/");

            if (Integer.parseInt(cur[0]) < Integer.parseInt(data[0])){
                return true;
            } else if (Integer.parseInt(cur[0]) == Integer.parseInt(data[0]) && Integer.parseInt(cur[1]) < Integer.parseInt(data[1])){
                return true;
            } else if (Integer.parseInt(cur[0]) == Integer.parseInt(data[0]) && Integer.parseInt(cur[1]) == Integer.parseInt(data[1]) && Integer.parseInt(cur[2]) < Integer.parseInt(data[2])){
                return true;
            }

            return false;
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "ReduceJoin"); // 实例化一道作业
        job.setJarByClass(ReduceJoin.class);

        Path inputPath1 = new Path(".\\src\\inputdata\\emp.csv");
        Path inputPath2 = new Path(".\\src\\inputdata\\dept.csv");
        Path outputPath = new Path(".\\src\\outputdata\\实验十\\JOIN(1)结果");


        Scanner scanner = new Scanner(System.in);
        System.out.println("1.求：部门名称 部门总工资\n2.求：部门名称 部门人数 部门平均工资\n3.求：部门名称 最早入职的员工姓名 入职日期");
        System.out.println("请选择[1\\2\\3]：");
        int a = scanner.nextInt();


        if (a == 1){
            outputPath = new Path(".\\src\\outputdata\\实验十\\JOIN(1)结果");

            // 指定job的mapper的输出的类型 k2 v2
            job.setMapperClass(ReduceJoinMapper.class); // 设置Mapper类
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 指定job的reducer的输出的类型 k4 v4
            job.setReducerClass(JoinOneReducer.class);// 设置ReducerClass类
            job.setOutputKeyClass(Text.class); // 输出key的类型
            job.setOutputValueClass(IntWritable.class);// 输出value的类型

        }else if (a == 2){

            outputPath = new Path(".\\src\\outputdata\\实验十\\JOIN(2)结果");
            System.out.println("配置第二问的mapreduce");

            // 指定job的mapper的输出的类型 k2 v2
            job.setMapperClass(ReduceJoinMapper.class); // 设置Mapper类
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 指定job的reducer的输出的类型 k4 v4
            job.setReducerClass(JoinTwoReducer.class);// 设置ReducerClass类
            job.setOutputKeyClass(Text.class); // 输出key的类型
            job.setOutputValueClass(Text.class);// 输出value的类型

        }else if (a == 3){

            outputPath = new Path(".\\src\\outputdata\\实验十\\JOIN(3)结果");
            System.out.println("配置第三问的mapreduce");

            // 指定job的mapper的输出的类型 k2 v2
            job.setMapperClass(ReduceJoinMapper.class); // 设置Mapper类
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 指定job的reducer的输出的类型 k4 v4
            job.setReducerClass(JoinThreeReducer.class);// 设置ReducerClass类
            job.setOutputKeyClass(Text.class); // 输出key的类型
            job.setOutputValueClass(Text.class);// 输出value的类型

        }


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
