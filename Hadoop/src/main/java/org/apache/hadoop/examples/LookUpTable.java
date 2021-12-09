package org.apache.hadoop.examples;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LookUpTable {
    public static class LookUpMapper extends Mapper<LongWritable, Text, Text, Log>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 数据样例： 20781048242866507	[earth]	5 1	download.it.com.cn/softweb/software/network/nethelper/20056/12277.html
            String data = value.toString();
            // 分词

            String[] words = data.split("\\s+");

            if (words.length != 5){
                return;
            }
//             创建对象
            Log e = new LookUpTable.Log();
//             设置属性
            e.set(words[0], words[1], Integer.parseInt(words[2]), Integer.parseInt(words[3]), words[4]);
//             mapper输出
            context.write(new Text(words[0]), e);

        }
    }

    public static class LookUpReducer extends Reducer<Text, Log, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Log> values, Context context) throws IOException, InterruptedException {
            for (Log e: values){
                if (e.rank == 2 && e.clickorder == 1){
                    context.write(key, new Text(e.word +" " + e.rank + " " + e.clickorder + " " + e.url));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "LookUpTable"); // 实例化一道作业
        job.setJarByClass(TopScore.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(LookUpTable.LookUpMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LookUpTable.Log.class);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(LookUpTable.LookUpReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(Text.class);// 输出key的类型
        job.setOutputValueClass(Text.class);// 输出value的类型


        // 集群
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 本地测试
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\98708\\Desktop\\access_log.20060805.decode.filter"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\98708\\Desktop\\output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Log implements Writable {
        // 数据类型：用户ID  [查询词]  该URL在返回结果中的排名  用户点击的顺序 用户点击的URL
        // 数据样例：20781048242866507	[earth]	5 1	download.it.com.cn/softweb/software/network/nethelper/20056/12277.html
        private String id; // 用户ID
        private String word;// [查询词]
        private int rank; // 排名
        private  int clickorder; // 用户点击的顺序
        private String url; // 用户点击的URL

        //序列化方法：将java对象转化为可跨机器传输数据流（二进制串/字节）的一种技术，通俗理解为写操作
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.id);
            out.writeUTF(this.word);
            out.writeInt(this.rank);
            out.writeInt(this.clickorder);
            out.writeUTF(this.url);

        }

        //反序列化方法：将可跨机器传输数据流（二进制串）转化为java对象的一种技术,通俗理解为读操作
        public void readFields(DataInput in) throws IOException {
            this.id = in.readUTF();
            this.word = in.readUTF();
            this.rank = in.readInt();
            this.clickorder= in.readInt();
            this.url = in.readUTF();
        }

        public void set(String id, String word, int rank, int clickorder, String url){
            this.id = id;
            this.word = word;
            this.rank = rank;
            this.clickorder = clickorder;
            this.url = url;
        }

        public String getId(){
            return id;
        }

        public String getWord(){
            return word;
        }

        public int getRank(){
            return rank;
        }


        public int getClickorder(){
            return clickorder;
        }

        public String getUrl() {
            return url;
        }
    }
}