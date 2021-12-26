package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

            context.write(k3, new Text(String.valueOf(total_sales) + " " + total_count));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "SalesStatistics"); // 实例化一道作业
        job.setJarByClass(SalesStatistics.class);

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

//一、概述
//        1.大数据特点：4V：Volume、Variety、Value和Velocity
//           大数据技术核心：可靠存储和高效计算
//        2.Google的三驾马车
//          2003：GFS《The Google File System》
//          2004：MapReduce  《MapReduce: Simplified Data Processing on Large Clusters》
//          2006：BigTable  《Bigtable: A Distributed Storage System for Structured Data》
//          GFS架构：GFS Master和Chunk Server
//          MapReduce思想：分而治之——分散任务，汇总结果
//          BigTable的数据模型：列族
//        3.Hadoop
//          Hadoop的HDFS、MapReduce、HBase分别是对Google公司的GFS、MapReduce、BigTable思想的开源实现。
//          HDFS架构：NameNode、DataNode、SecondareNameNode
//          机架感知
//          Hadoop的发展史
//          Hadoop2.0比1.0：增加了YARN；还支持HDFS的Federation（联邦）、HA（High Availability）等
//          Hadoop生态圈：HDFS、Yarn、MapReduce
//
//        二、Hadoop集群部署
//          Hadoop的三种安装模式：单机模式、伪分布模式、完全分布式模式
//          下面进程都将启动：
//          HDFS:
//            Namenode（master主节点进程）
//            Datanode（slave从节点进程）
//            SecondaryNamenode （master主节点进程）
//          Yarn:
//           Resourcemanager （master主节点进程）
//           Nodemanager （slave从节点进程）
//
//          安装准备工作：
//          网络环境的配置（主机名hostname的配置、网络ip地址和hostname的映射关系）
//          设置SSH免密登录（完全分布式模式中，任意两个节点之间均需要设置）
//          1）生成密钥对：
//          # ssh-keygen –t rsa
//          2）复制密钥id_rsa.pub为authorized_keys
//          # cp id_rsa.pub authorized_keys
//          3）免密登录的测试
//          # ssh namenode
//          安装jdk：配置环境变量并生效
//          安装hadoop
//          设置Hadoop配置文件，所有需要设置的配置文件的路径位于目录：
//          ${HADOOP_HOME}/etc/hadoop
//          hadoop-env.sh  core-site.xml  hdfs-site.xml  mapred-site.xml  yarn-site.xml  slaves
//          HDFS格式化及启停
//          1)格式化文件系统
//          # bin/hdfs namenode –format
//          2)启动HDFS
//          # sbin/start-dfs.sh
//          # sbin/start-yarn.sh
//          3)检测是否启动成功
//          # jps
//
//        三、HDFS
//          HDFS文件系统的设计思想、基本架构：
//          其中，NameNode、SecondaryNameNode和DataNode各自职责
//          HDFS读写数据流程：两个图
//          FileSystem    FSDataInputStream   FSDataOutputStream
//          HDFS Shell命令：mkdir、ls、cat……   （实验二）
//          Java API操作HDFS    （实验三）
//
//          四、Yarn
//          YARN的架构：主从架构 图4-6
//          ResourceManager、NodeManager、Container、ApplicationMaster
//          Yarn中应用运行机制：图4-7
//
//        五、MapReduce
//          MapRuduce思想：分而治之
//          MapReduce编程模型   键值对(k,v)
//          Mapper、Reducer、Combiner、Partitioner组件
//          MapRuduce的工作机制 图5-11
//          Shuffle的工作过程 图5-13