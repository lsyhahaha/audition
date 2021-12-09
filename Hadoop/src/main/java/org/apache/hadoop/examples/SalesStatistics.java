package org.apache.hadoop.examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesStatistics {

    public static class SalesTotalMapper extends Mapper<LongWritable, Text, Text, Order> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // 数据样例：13,987	,1998-1-10,	3	999	1	1232.16
            String data = value.toString();
            // 分词
            String[] words = data.split(",");
            // ["13", "987", .... "123.16"]
            // 创建订单对象
            Order e = new SalesStatistics.Order();
            // 设置订单属性,产品ID，客户ID，订单日期，渠道ID，促销ID，销售数量，销售总额
            e.set(Integer.parseInt(words[0]), Integer.parseInt(words[1]), words[2], Integer.parseInt(words[3]), Integer.parseInt(words[4]), Integer.parseInt(words[5]), Double.parseDouble(words[6]));
            String year = words[2].split("-")[0];

            context.write(new Text(year), e);
        }
    }

    public static class SalesTotalReducer extends Reducer<Text, Order, Text, Text> {
        @Override
        protected void reduce(Text k3, Iterable<Order> v3, Context context) throws IOException, InterruptedException {
            int total_sales = 0;
            double total_count = 0;

            for (Order e: v3){
                total_sales = total_sales + e.getSalesVolume();
                total_count = total_count + e.getSalesCount();
            }

//            double total_count = 0;
//            for (Order e: v3){
//                System.out.println("标记：" + e.getSalesCount());
//                total_count = total_count + e.getSalesCount();
//            }

            context.write(k3, new Text(String.valueOf(total_sales) + " " + total_count));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "SalesStatistics"); // 实例化一道作业
        job.setJarByClass(org.apache.hadoop.examples.SalesStatistics.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(SalesStatistics.SalesTotalMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesStatistics.Order.class);


        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(SalesStatistics.SalesTotalReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(Text.class);// 输出key的类型， 部门号
        job.setOutputValueClass(Text.class);// 输出value的类型， 员工

        Path inputPath = new Path(".\\src\\inputdata\\sales.csv");
        Path outputPath = new Path(".\\src\\outputdata\\实验七SalesStatistics");

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

    public static class Order implements Writable {
        // 订单类
        /*
        * 订单的数据描述如下：产品ID，客户ID，订单日期，渠道ID，促销ID，销售数量，销售总额
        * */
        // 数据样例 13	987	1998-1-10	3	999	 1	1232.16
        private int pid;// 产品ID(product)
        private int cid;// 客户ID(client)
        private String hiredate; // 订单日期
        private int chid;// 渠道ID(channel)
        private int prid;// 促销ID(Promotion)
        private int  SalesVolume;// 销售数量
        private double SalesCount;// 销售总额


        //序列化方法：将java对象转化为可跨机器传输数据流（二进制串/字节）的一种技术，通俗理解为写操作
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.pid);
            out.writeInt(this.cid);
            out.writeUTF(this.hiredate);
            out.writeInt(this.chid);
            out.writeInt(this.prid);
            out.writeInt(this.SalesVolume);
            out.writeDouble(this.SalesCount);

        }
        //反序列化方法：将可跨机器传输数据流（二进制串）转化为java对象的一种技术,通俗理解为读操作
        public void readFields(DataInput in) throws IOException {
            this.pid = in.readInt();
            this.cid = in.readInt();
            this.hiredate = in.readUTF();
            this.chid = in.readInt();
            this.prid = in.readInt();
            this.SalesVolume = in.readInt();
            this.SalesCount = in.readDouble();
        }

        //其他类通过set/get方法操作变量：Source-->Generator Getters and Setters
        public int getPid() {
            return pid;
        }
        public int getCid() {
            return cid;
        }
        public String getHiredate(){
            return hiredate;
        }
        public int getChid(){
            return chid;
        }
        public int getPrid(){
            return prid;
        }
        public int getSalesVolume(){
            return SalesVolume;
        }
        public double getSalesCount(){
            return SalesCount;
        }

        public void set(int pid, int cid, String hiredate, int chid, int prid, int SalesVolume, double SalesCount){
            this.pid = pid;
            this.cid = cid;
            this.hiredate = hiredate;
            this.chid = chid;
            this.prid = prid;
            this.SalesVolume = SalesVolume;
            this.SalesCount = SalesCount;
        }

    }
}